#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/client/rpc_client.h"
#include "minirpc/core/status.h"
#include "minirpc/net/tcp_client.h"
#include "minirpc/protocol/body_codec.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/server/coroutine_rpc_server.h"

namespace {

constexpr int32_t kOk = static_cast<int32_t>(minirpc::StatusCode::kOk);
constexpr int32_t kServerError = static_cast<int32_t>(minirpc::StatusCode::kServerError);
constexpr int32_t kSerializeError = static_cast<int32_t>(minirpc::StatusCode::kSerializeError);

bool ContainsLine(const std::string& text, const std::string& line) {
    return text.find(line + "\n") != std::string::npos;
}

int ConnectRawWithReceiveBuffer(uint16_t port, int receive_buffer_bytes) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(fd != -1);
    assert(::setsockopt(fd,
                        SOL_SOCKET,
                        SO_RCVBUF,
                        &receive_buffer_bytes,
                        sizeof(receive_buffer_bytes)) == 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    assert(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
    assert(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    return fd;
}

void SendAllRaw(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (n == -1 && errno == EINTR) {
            continue;
        }
        assert(n > 0);
        sent += static_cast<std::size_t>(n);
    }
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

    assert(WaitUntil([&] {
        return server.metrics().total_responses() == 3 &&
               server.metrics().pending_requests() == 0;
    }, std::chrono::seconds(1)));
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

void TestCoroutineRpcServerCanRestart() {
    constexpr uint16_t kPort = 19625;

    minirpc::CoroutineRpcServer server(2, 32);
    server.RegisterService("EchoService", "Echo", EchoHandler);

    for (int attempt = 0; attempt < 2; ++attempt) {
        assert(server.Start({"127.0.0.1", kPort}).ok());

        minirpc::RpcClient client({"127.0.0.1", kPort});
        const auto response = client.Call(
            "EchoService", "Echo", "restart-" + std::to_string(attempt),
            std::chrono::seconds(2));
        assert(response.status_code == kOk);
        assert(response.payload == "restart-" + std::to_string(attempt));
        client.Close();

        server.Stop();
        assert(!server.running());
        assert(server.metrics().active_connections() == 0);
    }
}

void TestCoroutineRpcServerReclaimsConnectionChurn() {
    constexpr uint16_t kPort = 19630;
    constexpr int kConnectionCount = 1000;

    minirpc::CoroutineRpcServer server(2, 32);
    assert(server.Start({"127.0.0.1", kPort}).ok());

    for (int i = 0; i < kConnectionCount; ++i) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        assert(fd != -1);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(kPort);
        assert(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
        assert(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        ::close(fd);
    }

    assert(WaitUntil([&] { return server.metrics().active_connections() == 0; },
                     std::chrono::seconds(3)));
    server.Stop();
    assert(server.metrics().active_connections() == 0);
    assert(server.metrics().pending_requests() == 0);
}

void TestCoroutineRpcServerRejectsBufferedRequestWhileDraining() {
    constexpr uint16_t kPort = 19626;

    std::promise<void> handler_entered;
    auto entered = handler_entered.get_future();
    std::promise<void> release_handler;
    auto release = release_handler.get_future().share();

    minirpc::CoroutineRpcServer server(2, 32);
    server.RegisterService("EchoService", "Slow", [&](const minirpc::RpcRequest& request) {
        handler_entered.set_value();
        release.wait();
        return EchoHandler(request);
    });
    server.RegisterService("EchoService", "Echo", EchoHandler);
    assert(server.Start({"127.0.0.1", kPort}).ok());

    minirpc::RpcClient client({"127.0.0.1", kPort});
    auto first = client.CallAsync(
        "EchoService", "Slow", "first", std::chrono::seconds(3));
    assert(entered.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    auto buffered = client.CallAsync(
        "EchoService", "Echo", "buffered", std::chrono::seconds(3));

    std::thread first_stopper([&] { server.Stop(std::chrono::seconds(1)); });
    assert(WaitUntil([&] { return !server.running(); }, std::chrono::seconds(1)));

    std::atomic<bool> second_stop_returned{false};
    std::thread second_stopper([&] {
        server.Stop(std::chrono::seconds(1));
        second_stop_returned.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    assert(!second_stop_returned.load());

    release_handler.set_value();
    const auto first_response = first.get();
    const auto buffered_response = buffered.get();
    first_stopper.join();
    second_stopper.join();

    assert(first_response.status_code == kOk);
    assert(buffered_response.status_code == kServerError);
    assert(buffered_response.error_message == "server shutting down");
    assert(second_stop_returned.load());
    assert(server.metrics().rejected_requests() == 1);
    assert(server.metrics().pending_requests() == 0);
    assert(server.metrics().active_connections() == 0);
}

void TestCoroutineRpcServerHandlesOversizedResponse() {
    constexpr uint16_t kPort = 19627;

    minirpc::CoroutineRpcServer server(2, 32);
    server.RegisterService("EchoService", "Oversized", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.status_code = kOk;
        response.payload.assign(minirpc::kDefaultMaxFrameBodySize, 'x');
        return response;
    });
    server.RegisterService("EchoService", "Echo", EchoHandler);
    assert(server.Start({"127.0.0.1", kPort}).ok());

    minirpc::RpcClient client({"127.0.0.1", kPort});
    const auto oversized = client.Call(
        "EchoService", "Oversized", "", std::chrono::seconds(3));
    assert(oversized.status_code == kSerializeError);
    assert(oversized.error_message == "response body too large");

    const auto echo = client.Call(
        "EchoService", "Echo", "still-alive", std::chrono::seconds(2));
    assert(echo.status_code == kOk);
    assert(echo.payload == "still-alive");

    client.Close();
    server.Stop();
    assert(server.metrics().pending_requests() == 0);
    assert(server.metrics().active_connections() == 0);
}

void TestCoroutineRpcServerHardStopCompletesInflightRequest() {
    constexpr uint16_t kPort = 19628;

    std::promise<void> handler_entered;
    auto entered = handler_entered.get_future();
    std::promise<void> release_handler;
    auto release = release_handler.get_future().share();

    minirpc::CoroutineRpcServer server(1, 8);
    server.RegisterService("EchoService", "Slow", [&](const minirpc::RpcRequest& request) {
        handler_entered.set_value();
        release.wait();
        return EchoHandler(request);
    });
    assert(server.Start({"127.0.0.1", kPort}).ok());

    minirpc::RpcClient client({"127.0.0.1", kPort});
    auto response = client.CallAsync(
        "EchoService", "Slow", "hard-stop", std::chrono::seconds(3));
    assert(entered.wait_for(std::chrono::seconds(1)) == std::future_status::ready);

    std::thread releaser([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        release_handler.set_value();
    });
    server.Stop(std::chrono::milliseconds::zero());
    releaser.join();

    assert(response.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    assert(response.get().status_code != kOk);
    assert(server.metrics().graceful_shutdown_timeout_count() == 1);
    assert(server.metrics().shutdown_start_time_ms() > 0);
    assert(server.metrics().server_state() == 2);
    assert(server.metrics().pending_requests() == 0);
    assert(server.metrics().active_connections() == 0);
}

void TestCoroutineRpcServerHardStopCancelsSlowWrite() {
    constexpr uint16_t kPort = 19629;
    constexpr std::size_t kResponseBytes = 8 * 1024 * 1024;

    minirpc::CoroutineRpcServer server(1, 8);
    server.RegisterService("EchoService", "Large", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.status_code = kOk;
        response.payload.assign(kResponseBytes, 'w');
        return response;
    });
    assert(server.Start({"127.0.0.1", kPort}).ok());

    const int fd = ConnectRawWithReceiveBuffer(kPort, 4096);
    minirpc::RpcRequest request;
    request.request_id = 42;
    request.service_name = "EchoService";
    request.method_name = "Large";

    minirpc::ProtocolFrame request_frame;
    request_frame.request_id = request.request_id;
    request_frame.message_type = minirpc::MessageType::kRequest;
    request_frame.codec_type = minirpc::CodecType::kProtobuf;
    request_frame.body = minirpc::EncodeRequestBody(request);
    minirpc::RpcCodec codec;
    SendAllRaw(fd, codec.Encode(request_frame));

    assert(WaitUntil([&] { return server.metrics().pending_requests() == 1; },
                     std::chrono::seconds(2)));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto stop_start = std::chrono::steady_clock::now();
    server.Stop(std::chrono::milliseconds::zero());
    const auto stop_elapsed = std::chrono::steady_clock::now() - stop_start;
    ::close(fd);

    assert(stop_elapsed < std::chrono::seconds(2));
    assert(server.metrics().graceful_shutdown_timeout_count() == 1);
    assert(server.metrics().pending_requests() == 0);
    assert(server.metrics().active_connections() == 0);
    assert(server.metrics().total_responses() == 0);
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
    assert(WaitUntil([&] {
        return server.metrics().total_responses() == 1 &&
               server.metrics().pending_requests() == 0;
    }, std::chrono::seconds(1)));
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
    TestCoroutineRpcServerCanRestart();
    TestCoroutineRpcServerReclaimsConnectionChurn();
    TestCoroutineRpcServerRejectsBufferedRequestWhileDraining();
    TestCoroutineRpcServerHandlesOversizedResponse();
    TestCoroutineRpcServerHardStopCompletesInflightRequest();
    TestCoroutineRpcServerHardStopCancelsSlowWrite();
    TestCoroutineRpcServerRejectsExpiredDeadlineBeforeHandler();
    TestCoroutineRpcServerGracefulStopLetsInflightRequestFinish();
    TestCoroutineRpcServerRecordsBackpressureRejections();
    return 0;
}
