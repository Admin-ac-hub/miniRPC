#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "minirpc/client/rpc_client.h"
#include "minirpc/core/status.h"
#include "minirpc/net/tcp_client.h"
#include "minirpc/protocol/body_codec.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/server/coroutine_rpc_server.h"

namespace {

constexpr int32_t kOk = static_cast<int32_t>(minirpc::StatusCode::kOk);

bool ContainsLine(const std::string& text, const std::string& line) {
    return text.find(line + "\n") != std::string::npos;
}

template <typename Predicate>
bool WaitUntil(Predicate predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

int64_t UnixMsNow() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

minirpc::RpcResponse SendRawRequest(const minirpc::Endpoint& endpoint,
                                    const minirpc::RpcRequest& request) {
    std::promise<minirpc::RpcResponse> promise;
    auto future = promise.get_future();
    std::atomic<bool> done{false};
    minirpc::TcpClient client;
    client.SetOnFrame([&](minirpc::ProtocolFrame frame) {
        if (done.exchange(true)) {
            return;
        }
        promise.set_value(minirpc::DecodeResponseBody(frame.request_id, frame.body));
    });
    assert(client.Connect(endpoint).ok());

    minirpc::ProtocolFrame frame;
    frame.request_id = request.request_id;
    frame.message_type = minirpc::MessageType::kRequest;
    frame.codec_type = minirpc::CodecType::kProtobuf;
    frame.body = minirpc::EncodeRequestBody(request);
    assert(client.SendFrame(frame).ok());
    assert(future.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    client.Close();
    return future.get();
}

minirpc::RpcResponse EchoHandler(const minirpc::RpcRequest& request) {
    minirpc::RpcResponse response;
    response.request_id = request.request_id;
    response.status_code = kOk;
    response.payload = request.payload;
    return response;
}

void TestCoroutineRpcServerEchoAndErrors() {
    minirpc::CoroutineRpcServer server(2, 32);
    server.RegisterService("EchoService", "Echo", EchoHandler);
    assert(server.Start({"127.0.0.1", 19620}).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    minirpc::RpcClient client({"127.0.0.1", 19620});
    auto echo = client.Call("EchoService", "Echo", "hello", std::chrono::seconds(2));
    assert(echo.status_code == kOk);
    assert(echo.payload == "hello");

    auto missing_method = client.Call("EchoService", "Missing", "", std::chrono::seconds(2));
    assert(missing_method.status_code == static_cast<int32_t>(minirpc::StatusCode::kMethodNotFound));

    auto missing_service = client.Call("MissingService", "Echo", "", std::chrono::seconds(2));
    assert(missing_service.status_code == static_cast<int32_t>(minirpc::StatusCode::kServiceNotFound));

    assert(server.metrics().total_requests() == 3);
    assert(server.metrics().total_responses() == 3);
    assert(server.metrics().success_requests() == 1);
    assert(server.metrics().failed_requests() == 2);
    assert(server.metrics().pending_requests() == 0);
    assert(server.metrics().latency_samples() == 3);
    assert(server.threadpool_queue_size() == 0);
    const std::string metrics_text = server.MetricsText();
    assert(ContainsLine(metrics_text, "minirpc_requests_total 3"));
    assert(ContainsLine(metrics_text, "minirpc_responses_total 3"));
    assert(ContainsLine(metrics_text, "minirpc_success_requests_total 1"));
    assert(ContainsLine(metrics_text, "minirpc_failed_requests_total 2"));
    assert(ContainsLine(metrics_text, "minirpc_threadpool_queue_size 0"));

    client.Close();
    server.Stop();
    assert(!server.running());
    assert(server.metrics().active_connections() == 0);
}

void TestCoroutineRpcServerConcurrentClients() {
    minirpc::CoroutineRpcServer server(4, 64);
    server.RegisterService("EchoService", "Echo", EchoHandler);
    assert(server.Start({"127.0.0.1", 19621}).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    constexpr int kClients = 8;
    constexpr int kRequestsPerClient = 8;
    std::vector<std::thread> threads;
    std::atomic<int> ok_count{0};
    for (int i = 0; i < kClients; ++i) {
        threads.emplace_back([i, &ok_count] {
            minirpc::RpcClient client({"127.0.0.1", 19621});
            for (int j = 0; j < kRequestsPerClient; ++j) {
                const std::string payload = "co-" + std::to_string(i) + "-" + std::to_string(j);
                auto response = client.Call("EchoService", "Echo", payload, std::chrono::seconds(3));
                if (response.status_code == kOk && response.payload == payload) {
                    ok_count.fetch_add(1);
                }
            }
            client.Close();
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    assert(ok_count.load() == kClients * kRequestsPerClient);
    server.Stop();
}

void TestCoroutineRpcServerRejectsExpiredDeadlineBeforeHandler() {
    minirpc::CoroutineRpcServer server(2, 32);
    std::atomic<int> invoked{0};
    server.RegisterService("EchoService", "Echo", [&](const minirpc::RpcRequest& request) {
        invoked.fetch_add(1);
        return EchoHandler(request);
    });
    assert(server.Start({"127.0.0.1", 19622}).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    minirpc::RpcRequest request;
    request.request_id = 900;
    request.service_name = "EchoService";
    request.method_name = "Echo";
    request.payload = "expired";
    request.deadline_unix_ms = UnixMsNow() - 1000;

    auto response = SendRawRequest({"127.0.0.1", 19622}, request);
    assert(response.request_id == request.request_id);
    assert(response.status_code == static_cast<int32_t>(minirpc::StatusCode::kTimeout));
    assert(response.error_message == "request deadline exceeded");
    assert(invoked.load() == 0);
    assert(server.metrics().total_requests() == 1);
    assert(server.metrics().total_responses() == 1);
    assert(server.metrics().timeout_requests() == 1);
    assert(server.metrics().failed_requests() == 1);
    assert(server.metrics().success_requests() == 0);

    server.Stop();
}

void TestCoroutineRpcServerGracefulStopLetsInflightRequestFinish() {
    minirpc::CoroutineRpcServer server(2, 32);
    server.RegisterService("EchoService", "Echo", [](const minirpc::RpcRequest& request) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return EchoHandler(request);
    });
    assert(server.Start({"127.0.0.1", 19623}).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::atomic<int32_t> got_status{-1};
    std::string got_payload;
    std::thread caller([&] {
        minirpc::RpcClient client({"127.0.0.1", 19623});
        auto response = client.Call("EchoService", "Echo", "grace", std::chrono::seconds(2));
        got_status.store(response.status_code);
        got_payload = response.payload;
        client.Close();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    std::thread stopper([&] {
        server.Stop(std::chrono::milliseconds(1000));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    minirpc::RpcClient late_client({"127.0.0.1", 19623});
    auto late = late_client.Call("EchoService", "Echo", "late", std::chrono::milliseconds(200));
    assert(late.status_code == static_cast<int32_t>(minirpc::StatusCode::kNetworkError));
    late_client.Close();
    stopper.join();
    caller.join();

    assert(got_status.load() == kOk);
    assert(got_payload == "grace");
    assert(server.metrics().pending_requests() == 0);
    assert(server.metrics().success_requests() == 1);
}

void TestCoroutineRpcServerRecordsBackpressureRejections() {
    minirpc::CoroutineRpcServer server(1, 1);
    server.RegisterService("SlowService", "Work", [](const minirpc::RpcRequest& request) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        return EchoHandler(request);
    });
    assert(server.Start({"127.0.0.1", 19624}).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    constexpr int kClients = 8;
    std::atomic<int> rejected{0};
    std::atomic<int> ok{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kClients; ++i) {
        threads.emplace_back([i, &rejected, &ok] {
            minirpc::RpcClient client({"127.0.0.1", 19624});
            auto response = client.Call(
                "SlowService", "Work", "load-" + std::to_string(i), std::chrono::seconds(3));
            if (response.status_code == static_cast<int32_t>(minirpc::StatusCode::kServerError)) {
                rejected.fetch_add(1);
            }
            if (response.status_code == kOk) {
                ok.fetch_add(1);
            }
            client.Close();
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    assert(rejected.load() > 0);
    assert(ok.load() > 0);
    const int completed = ok.load() + rejected.load();
    assert(completed == kClients);
    assert(WaitUntil([&] {
        return server.metrics().pending_requests() == 0 &&
               server.metrics().total_responses() == static_cast<uint64_t>(completed);
    }, std::chrono::seconds(1)));
    assert(server.metrics().total_responses() == static_cast<uint64_t>(completed));
    assert(server.metrics().rejected_requests() == static_cast<uint64_t>(rejected.load()));
    assert(server.metrics().failed_requests() >= static_cast<uint64_t>(rejected.load()));
    assert(server.metrics().pending_requests() == 0);

    server.Stop();
}

}  // namespace

int main() {
    TestCoroutineRpcServerEchoAndErrors();
    TestCoroutineRpcServerConcurrentClients();
    TestCoroutineRpcServerRejectsExpiredDeadlineBeforeHandler();
    TestCoroutineRpcServerGracefulStopLetsInflightRequestFinish();
    TestCoroutineRpcServerRecordsBackpressureRejections();
    return 0;
}
