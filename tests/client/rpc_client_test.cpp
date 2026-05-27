#include <atomic>
#include <cassert>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "minirpc/client/rpc_client.h"
#include "minirpc/core/status.h"
#include "minirpc/protocol/message.h"
#include "minirpc/server/rpc_server.h"

namespace {

#ifdef __linux__

constexpr int32_t kOk = static_cast<int32_t>(minirpc::StatusCode::kOk);
constexpr int32_t kTimeout = static_cast<int32_t>(minirpc::StatusCode::kTimeout);
constexpr int32_t kNetworkError = static_cast<int32_t>(minirpc::StatusCode::kNetworkError);

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

    client.Close();
    server.Stop();
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

    client.Close();
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

#endif  // __linux__

}  // namespace

int main() {
#ifdef __linux__
    TestBasicEcho();
    TestTimeoutCleanup();
    TestConcurrentCalls();
    TestCloseFailsPending();
    TestServerNotRunning();
#endif
    return 0;
}
