#include <atomic>
#include <cassert>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "minirpc/net/tcp_client.h"
#include "minirpc/net/tcp_server.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"

namespace {

void TestSingleEcho() {
    minirpc::TcpServer server;
    server.SetOnFrame([&server](minirpc::ConnectionId id, uint64_t gen, minirpc::ProtocolFrame frame) {
        minirpc::ProtocolFrame resp = frame;
        resp.message_type = minirpc::MessageType::kResponse;
        (void)server.SendFrame(id, gen, resp, false);
    });
    auto s = server.Start({"127.0.0.1", 19400});
    assert(s.ok());

    std::mutex m;
    std::vector<std::string> received;
    minirpc::TcpClient client;
    client.SetOnFrame([&](minirpc::ProtocolFrame frame) {
        std::lock_guard<std::mutex> lock(m);
        received.push_back(frame.body);
    });
    auto c = client.Connect({"127.0.0.1", 19400});
    assert(c.ok());

    minirpc::ProtocolFrame frame;
    frame.request_id = 1;
    frame.message_type = minirpc::MessageType::kRequest;
    frame.body = "hello";
    assert(client.SendFrame(frame).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    {
        std::lock_guard<std::mutex> lock(m);
        assert(received.size() == 1);
        assert(received[0] == "hello");
    }
    client.Close();
    server.Stop();
}

void TestLargeFrame() {
    minirpc::TcpServer server;
    server.SetOnFrame([&server](minirpc::ConnectionId id, uint64_t gen, minirpc::ProtocolFrame frame) {
        minirpc::ProtocolFrame resp = frame;
        resp.message_type = minirpc::MessageType::kResponse;
        (void)server.SendFrame(id, gen, resp, false);
    });
    auto s = server.Start({"127.0.0.1", 19401});
    assert(s.ok());

    std::mutex m;
    std::string received;
    minirpc::TcpClient client;
    client.SetOnFrame([&](minirpc::ProtocolFrame frame) {
        std::lock_guard<std::mutex> lock(m);
        received = frame.body;
    });
    auto c = client.Connect({"127.0.0.1", 19401});
    assert(c.ok());

    std::string payload(1024 * 1024, 'a');  // ~1 MB
    minirpc::ProtocolFrame frame;
    frame.request_id = 2;
    frame.message_type = minirpc::MessageType::kRequest;
    frame.body = payload;
    assert(client.SendFrame(frame).ok());

    for (int i = 0; i < 50; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::lock_guard<std::mutex> lock(m);
        if (received.size() == payload.size()) break;
    }
    {
        std::lock_guard<std::mutex> lock(m);
        assert(received == payload);
    }
    client.Close();
    server.Stop();
}

void TestConcurrentClients() {
    minirpc::TcpServer server;
    server.SetOnFrame([&server](minirpc::ConnectionId id, uint64_t gen, minirpc::ProtocolFrame frame) {
        minirpc::ProtocolFrame resp = frame;
        resp.message_type = minirpc::MessageType::kResponse;
        (void)server.SendFrame(id, gen, resp, false);
    });
    auto s = server.Start({"127.0.0.1", 19402});
    assert(s.ok());

    constexpr int kN = 20;
    std::vector<std::thread> threads;
    std::atomic<int> success{0};
    for (int i = 0; i < kN; ++i) {
        threads.emplace_back([i, &success] {
            std::mutex m;
            std::string got;
            minirpc::TcpClient client;
            client.SetOnFrame([&](minirpc::ProtocolFrame frame) {
                std::lock_guard<std::mutex> lock(m);
                got = frame.body;
            });
            auto c = client.Connect({"127.0.0.1", 19402});
            assert(c.ok());
            const std::string msg = "msg-" + std::to_string(i);
            minirpc::ProtocolFrame frame;
            frame.request_id = static_cast<uint64_t>(i) + 100;
            frame.message_type = minirpc::MessageType::kRequest;
            frame.body = msg;
            assert(client.SendFrame(frame).ok());
            for (int j = 0; j < 50; ++j) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                std::lock_guard<std::mutex> lock(m);
                if (got == msg) { success.fetch_add(1); break; }
            }
            client.Close();
        });
    }
    for (auto& t : threads) t.join();
    assert(success.load() == kN);
    server.Stop();
}

void TestServerObservesClientClose() {
    minirpc::TcpServer server;
    std::atomic<int> close_count{0};
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    server.SetOnFrame([](minirpc::ConnectionId, uint64_t, minirpc::ProtocolFrame) {});
    auto s = server.Start({"127.0.0.1", 19403});
    assert(s.ok());

    {
        minirpc::TcpClient client;
        auto c = client.Connect({"127.0.0.1", 19403});
        assert(c.ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        client.Close();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    assert(close_count.load() == 1);
    server.Stop();
}

}  // namespace

int main() {
    TestSingleEcho();
    TestLargeFrame();
    TestConcurrentClients();
    TestServerObservesClientClose();
    return 0;
}
