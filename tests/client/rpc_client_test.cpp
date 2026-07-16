#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "minirpc/client/rpc_client.h"
#include "minirpc/core/status.h"
#include "minirpc/net/tcp_client.h"
#include "minirpc/protocol/body_codec.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/protocol/message.h"
#include "minirpc/server/rpc_server.h"

namespace {

constexpr int32_t kOk = static_cast<int32_t>(minirpc::StatusCode::kOk);
constexpr int32_t kTimeout = static_cast<int32_t>(minirpc::StatusCode::kTimeout);
constexpr int32_t kNetworkError = static_cast<int32_t>(minirpc::StatusCode::kNetworkError);

bool ContainsLine(const std::string& text, const std::string& line) {
    return text.find(line + "\n") != std::string::npos;
}

int64_t UnixMsNow() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
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

minirpc::RpcResponse EchoHandler(const minirpc::RpcRequest& req) {
    minirpc::RpcResponse resp;
    resp.request_id = req.request_id;
    resp.status_code = kOk;
    resp.payload = req.payload;
    return resp;
}

minirpc::RpcResponse SlowHandler(const minirpc::RpcRequest& req) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    return EchoHandler(req);
}

void TestBasicEcho() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", EchoHandler);
    assert(server.Start({"127.0.0.1", 19500}).ok());

    minirpc::RpcClient client({"127.0.0.1", 19500});
    auto resp = client.Call("EchoService", "Echo", "hello", std::chrono::seconds(2));
    assert(resp.status_code == kOk);
    assert(resp.payload == "hello");
    assert(server.metrics().total_requests() == 1);
    assert(server.metrics().success_requests() == 1);
    assert(server.metrics().failed_requests() == 0);
    assert(server.metrics().pending_requests() == 0);
    assert(server.metrics().latency_samples() == 1);
    assert(server.threadpool_queue_size() == 0);
    const std::string metrics_text = server.MetricsText();
    assert(ContainsLine(metrics_text, "minirpc_requests_total 1"));
    assert(ContainsLine(metrics_text, "minirpc_responses_total 1"));
    assert(ContainsLine(metrics_text, "minirpc_success_requests_total 1"));
    assert(ContainsLine(metrics_text, "minirpc_p95_latency_us 0") ||
           metrics_text.find("minirpc_p95_latency_us ") != std::string::npos);
    assert(ContainsLine(metrics_text, "minirpc_backpressure_connections 0"));
    assert(ContainsLine(metrics_text, "minirpc_threadpool_queue_size 0"));

    auto metrics_resp = client.Call("rpc", "metrics", "", std::chrono::seconds(2));
    assert(metrics_resp.status_code == kOk);
    assert(metrics_resp.payload.find("minirpc_p95_latency_us ") != std::string::npos);
    assert(metrics_resp.payload.find("minirpc_backpressure_connections ") != std::string::npos);

    client.Close();
    server.Stop();
    assert(server.metrics().active_connections() == 0);
}

void TestTimeoutCleanup() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", SlowHandler);
    assert(server.Start({"127.0.0.1", 19501}).ok());

    minirpc::RpcClient client({"127.0.0.1", 19501});
    auto resp = client.Call("EchoService", "Echo", "x", std::chrono::milliseconds(50));
    assert(resp.status_code == kTimeout);

    // Late response must not crash. Give server time to deliver after timeout.
    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    // Next call must still work on same client (connection reused).
    auto resp2 = client.Call("EchoService", "Echo", "y", std::chrono::seconds(2));
    assert(resp2.status_code == kOk);
    assert(resp2.payload == "y");
    assert(server.metrics().total_requests() == 2);
    assert(server.metrics().success_requests() == 2);
    assert(server.metrics().pending_requests() == 0);

    client.Close();
    server.Stop();
}

void TestEarlierDeadlineWakesTimeoutCleaner() {
    minirpc::RpcServer server;
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool release_handlers = false;
    std::atomic<int> entered_handlers{0};
    server.RegisterService("EchoService", "Echo", [&](const minirpc::RpcRequest& request) {
        entered_handlers.fetch_add(1, std::memory_order_release);
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait(lock, [&] { return release_handlers; });
        return EchoHandler(request);
    });
    assert(server.Start({"127.0.0.1", 19511}).ok());

    minirpc::RpcClient client({"127.0.0.1", 19511});
    auto long_deadline = client.CallAsync(
        "EchoService", "Echo", "long", std::chrono::seconds(5));
    assert(WaitUntil([&] {
        return entered_handlers.load(std::memory_order_acquire) >= 1;
    }, std::chrono::seconds(1)));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto short_deadline = client.CallAsync(
        "EchoService", "Echo", "short", std::chrono::milliseconds(100));
    assert(short_deadline.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    assert(short_deadline.get().status_code == kTimeout);

    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        release_handlers = true;
    }
    gate_cv.notify_all();
    client.Close();
    assert(long_deadline.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    (void)long_deadline.get();
    server.Stop();
}

void TestConcurrentCalls() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", EchoHandler);
    assert(server.Start({"127.0.0.1", 19502}).ok());

    constexpr int kN = 32;
    std::vector<std::thread> threads;
    std::atomic<int> ok_count{0};
    std::atomic<int> mismatch_count{0};

    // One shared client to exercise pending_ map concurrency.
    minirpc::RpcClient client({"127.0.0.1", 19502});
    for (int i = 0; i < kN; ++i) {
        threads.emplace_back([i, &client, &ok_count, &mismatch_count] {
            const std::string payload = "p-" + std::to_string(i);
            auto resp = client.Call("EchoService", "Echo", payload, std::chrono::seconds(3));
            if (resp.status_code != kOk) return;
            if (resp.payload == payload) {
                ok_count.fetch_add(1);
            } else {
                mismatch_count.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) t.join();
    assert(mismatch_count.load() == 0);
    assert(ok_count.load() == kN);

    client.Close();
    server.Stop();
}

void TestCallAsyncConcurrentRequests() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", EchoHandler);
    assert(server.Start({"127.0.0.1", 19507}).ok());

    minirpc::RpcClient client({"127.0.0.1", 19507});
    constexpr int kN = 32;
    std::vector<std::future<minirpc::RpcResponse>> futures;
    futures.reserve(kN);
    for (int i = 0; i < kN; ++i) {
        futures.push_back(client.CallAsync("EchoService",
                                           "Echo",
                                           "async-" + std::to_string(i),
                                           std::chrono::seconds(3)));
    }

    for (int i = 0; i < kN; ++i) {
        assert(futures[static_cast<std::size_t>(i)].wait_for(std::chrono::seconds(3)) ==
               std::future_status::ready);
        auto response = futures[static_cast<std::size_t>(i)].get();
        assert(response.status_code == kOk);
        assert(response.payload == "async-" + std::to_string(i));
    }

    client.Close();
    server.Stop();
}

void TestReconnectAfterServerRestart() {
    const minirpc::Endpoint endpoint{"127.0.0.1", 19508};
    minirpc::RpcClient client(endpoint);
    minirpc::RpcClientOptions options;
    options.max_reconnect_attempts = 5;
    options.reconnect_interval = std::chrono::milliseconds(20);
    client.SetOptions(options);

    {
        minirpc::RpcServer server;
        server.RegisterService("EchoService", "Echo", EchoHandler);
        assert(server.Start(endpoint).ok());
        auto first = client.Call("EchoService", "Echo", "first", std::chrono::seconds(2));
        assert(first.status_code == kOk);
        assert(first.payload == "first");
        server.Stop();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    {
        minirpc::RpcServer server;
        server.RegisterService("EchoService", "Echo", EchoHandler);
        assert(server.Start(endpoint).ok());
        auto second = client.Call("EchoService", "Echo", "second", std::chrono::seconds(2));
        assert(second.status_code == kOk);
        assert(second.payload == "second");
        server.Stop();
    }

    client.Close();
}

void TestCloseFailsPending() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", SlowHandler);
    assert(server.Start({"127.0.0.1", 19503}).ok());

    minirpc::RpcClient client({"127.0.0.1", 19503});
    // Warm up the connection.
    auto warm = client.Call("EchoService", "Echo", "warm", std::chrono::seconds(2));
    assert(warm.status_code == kOk);

    std::atomic<int32_t> got_status{-1};
    std::thread caller([&] {
        auto r = client.Call("EchoService", "Echo", "in-flight", std::chrono::seconds(5));
        got_status.store(r.status_code);
    });

    // Let request reach server, then close from another thread.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    client.Close();
    caller.join();

    assert(got_status.load() == kNetworkError);
    server.Stop();
}

void TestServerNotRunning() {
    minirpc::RpcClient client({"127.0.0.1", 19599});  // nobody listening
    auto resp = client.Call("EchoService", "Echo", "x", std::chrono::milliseconds(200));
    assert(resp.status_code == kNetworkError);
}

void TestServerMetricsFailures() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", EchoHandler);
    assert(server.Start({"127.0.0.1", 19504}).ok());

    minirpc::RpcClient client({"127.0.0.1", 19504});
    auto missing_service = client.Call("MissingService", "Echo", "", std::chrono::seconds(2));
    assert(missing_service.status_code == static_cast<int32_t>(minirpc::StatusCode::kServiceNotFound));
    auto missing_method = client.Call("EchoService", "Missing", "", std::chrono::seconds(2));
    assert(missing_method.status_code == static_cast<int32_t>(minirpc::StatusCode::kMethodNotFound));

    assert(server.metrics().total_requests() == 2);
    assert(server.metrics().total_responses() == 2);
    assert(server.metrics().success_requests() == 0);
    assert(server.metrics().failed_requests() == 2);
    assert(server.metrics().pending_requests() == 0);

    client.Close();
    server.Stop();
}

void TestServerRejectsExpiredDeadlineBeforeHandler() {
    minirpc::RpcServer server;
    std::atomic<int> invoked{0};
    server.RegisterService("EchoService", "Echo", [&](const minirpc::RpcRequest& req) {
        invoked.fetch_add(1);
        return EchoHandler(req);
    });
    assert(server.Start({"127.0.0.1", 19505}).ok());

    minirpc::RpcRequest request;
    request.request_id = 700;
    request.service_name = "EchoService";
    request.method_name = "Echo";
    request.payload = "expired";
    request.deadline_unix_ms = UnixMsNow() - 1000;

    auto response = SendRawRequest({"127.0.0.1", 19505}, request);
    assert(response.request_id == request.request_id);
    assert(response.status_code == kTimeout);
    assert(response.error_message == "request deadline exceeded");
    assert(invoked.load() == 0);
    assert(server.metrics().total_requests() == 1);
    assert(server.metrics().total_responses() == 1);
    assert(server.metrics().timeout_requests() == 1);
    assert(server.metrics().failed_requests() == 1);
    assert(server.metrics().success_requests() == 0);

    server.Stop();
}

void TestGracefulStopLetsInflightRequestFinish() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", [](const minirpc::RpcRequest& req) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        return EchoHandler(req);
    });
    assert(server.Start({"127.0.0.1", 19506}).ok());
    assert(server.shutdown_state() == minirpc::ShutdownState::kRunning);

    std::atomic<int32_t> got_status{-1};
    std::string got_payload;
    std::thread caller([&] {
        minirpc::RpcClient client({"127.0.0.1", 19506});
        auto response = client.Call("EchoService", "Echo", "grace", std::chrono::seconds(2));
        got_status.store(response.status_code);
        got_payload = response.payload;
        client.Close();
    });

    assert(WaitUntil([&] {
        return server.metrics().pending_requests() == 1;
    }, std::chrono::seconds(1)));
    std::thread stopper([&] {
        server.Stop(std::chrono::milliseconds(1000));
    });
    assert(WaitUntil([&] {
        return server.shutdown_state() == minirpc::ShutdownState::kDraining;
    }, std::chrono::seconds(1)));
    minirpc::RpcClient late_client({"127.0.0.1", 19506});
    auto late = late_client.Call("EchoService", "Echo", "late", std::chrono::milliseconds(200));
    assert(late.status_code == kNetworkError);
    late_client.Close();
    stopper.join();
    caller.join();

    assert(got_status.load() == kOk);
    assert(got_payload == "grace");
    assert(server.metrics().pending_requests() == 0);
    assert(server.metrics().success_requests() == 1);
    assert(server.shutdown_state() == minirpc::ShutdownState::kStopped);
    const std::string metrics_text = server.MetricsText();
    assert(ContainsLine(metrics_text, "minirpc_server_state 2"));
    assert(ContainsLine(metrics_text, "minirpc_inflight_requests 0"));
    assert(ContainsLine(metrics_text, "minirpc_graceful_shutdown_timeout_total 0"));
}

void TestGracePeriodTimeoutIsRecorded() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", [](const minirpc::RpcRequest& req) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        return EchoHandler(req);
    });
    assert(server.Start({"127.0.0.1", 19509}).ok());

    std::thread caller([&] {
        minirpc::RpcClient client({"127.0.0.1", 19509});
        (void)client.Call("EchoService", "Echo", "slow", std::chrono::seconds(2));
        client.Close();
    });

    assert(WaitUntil([&] {
        return server.metrics().pending_requests() == 1;
    }, std::chrono::seconds(1)));
    const auto stop_start = std::chrono::steady_clock::now();
    server.Stop(std::chrono::milliseconds(20));
    const auto stop_elapsed = std::chrono::steady_clock::now() - stop_start;
    caller.join();

    assert(stop_elapsed < std::chrono::seconds(2));
    assert(server.shutdown_state() == minirpc::ShutdownState::kStopped);
    assert(server.metrics().graceful_shutdown_timeout_count() == 1);
    assert(server.MetricsText().find("minirpc_shutdown_start_time_ms ") != std::string::npos);
}

void TestDrainingRejectsNewRequestsOnExistingConnection() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", [](const minirpc::RpcRequest& req) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        return EchoHandler(req);
    });
    assert(server.Start({"127.0.0.1", 19510}).ok());

    minirpc::RpcClient client({"127.0.0.1", 19510});
    auto inflight = client.CallAsync("EchoService", "Echo", "inflight", std::chrono::seconds(2));
    assert(WaitUntil([&] {
        return server.metrics().pending_requests() == 1;
    }, std::chrono::seconds(1)));

    std::thread stopper([&] {
        server.Stop(std::chrono::milliseconds(1000));
    });
    assert(WaitUntil([&] {
        return server.shutdown_state() == minirpc::ShutdownState::kDraining;
    }, std::chrono::seconds(1)));

    auto rejected = client.Call("EchoService", "Echo", "new", std::chrono::seconds(1));
    assert(rejected.status_code == static_cast<int32_t>(minirpc::StatusCode::kServerError));
    assert(rejected.error_message == "server shutting down");

    assert(inflight.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    auto completed = inflight.get();
    assert(completed.status_code == kOk);
    assert(completed.payload == "inflight");

    stopper.join();
    client.Close();
    assert(server.metrics().total_responses() == 2);
    assert(server.metrics().rejected_requests() == 1);
    assert(server.metrics().success_requests() == 1);
}

}  // namespace

int main() {
    TestBasicEcho();
    TestTimeoutCleanup();
    TestEarlierDeadlineWakesTimeoutCleaner();
    TestConcurrentCalls();
    TestCallAsyncConcurrentRequests();
    TestReconnectAfterServerRestart();
    TestCloseFailsPending();
    TestServerNotRunning();
    TestServerMetricsFailures();
    TestServerRejectsExpiredDeadlineBeforeHandler();
    TestGracefulStopLetsInflightRequestFinish();
    TestGracePeriodTimeoutIsRecorded();
    TestDrainingRejectsNewRequestsOnExistingConnection();
    return 0;
}
