#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "minirpc/net/tcp_client.h"
#include "minirpc/net/tcp_server.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"

namespace {

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

int ConnectRaw(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(fd != -1);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    const int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    assert(rc == 0);
    return fd;
}

void SendAllRaw(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        assert(n > 0);
        sent += static_cast<std::size_t>(n);
    }
}

void SetRecvTimeout(int fd, int milliseconds) {
    timeval timeout{};
    timeout.tv_sec = milliseconds / 1000;
    timeout.tv_usec = (milliseconds % 1000) * 1000;
    const int rc = ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    assert(rc == 0);
}

std::vector<minirpc::ProtocolFrame> ReceiveFramesRaw(int fd, std::size_t frame_count) {
    minirpc::RpcCodec codec;
    std::string buffer;
    std::vector<minirpc::ProtocolFrame> frames;
    frames.reserve(frame_count);

    while (frames.size() < frame_count) {
        minirpc::ProtocolFrame frame;
        std::string error;
        const minirpc::DecodeResult result = codec.TryDecode(buffer, &frame, &error);
        assert(result != minirpc::DecodeResult::kProtocolError);
        if (result == minirpc::DecodeResult::kSuccess) {
            frames.push_back(std::move(frame));
            continue;
        }

        char chunk[4096];
        const ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
        assert(n > 0);
        buffer.append(chunk, static_cast<std::size_t>(n));
    }
    return frames;
}

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

void TestLargePayloadBurstPreservesOrder() {
    minirpc::TcpServer server;
    server.SetOnFrame([&server](minirpc::ConnectionId id, uint64_t gen, minirpc::ProtocolFrame frame) {
        minirpc::ProtocolFrame resp = frame;
        resp.message_type = minirpc::MessageType::kResponse;
        (void)server.SendFrame(id, gen, resp, false);
    });
    assert(server.Start({"127.0.0.1", 19404}).ok());

    constexpr int kFrames = 32;
    const std::string payload(64 * 1024, 'L');
    std::mutex m;
    std::vector<std::pair<uint64_t, std::string>> received;
    minirpc::TcpClient client;
    client.SetOnFrame([&](minirpc::ProtocolFrame frame) {
        std::lock_guard<std::mutex> lock(m);
        received.emplace_back(frame.request_id, std::move(frame.body));
    });
    assert(client.Connect({"127.0.0.1", 19404}).ok());

    for (int i = 0; i < kFrames; ++i) {
        minirpc::ProtocolFrame frame;
        frame.request_id = static_cast<uint64_t>(i + 1);
        frame.message_type = minirpc::MessageType::kRequest;
        frame.body = payload;
        assert(client.SendFrame(frame).ok());
    }

    assert(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(m);
        return received.size() == kFrames;
    }, std::chrono::seconds(5)));
    {
        std::lock_guard<std::mutex> lock(m);
        for (int i = 0; i < kFrames; ++i) {
            assert(received[static_cast<std::size_t>(i)].first == static_cast<uint64_t>(i + 1));
            assert(received[static_cast<std::size_t>(i)].second == payload);
        }
    }
    client.Close();
    server.Stop();
}

void TestSmallPacketHighFrequencyEchoPreservesOrder() {
    minirpc::TcpServer server;
    server.SetOnFrame([&server](minirpc::ConnectionId id, uint64_t gen, minirpc::ProtocolFrame frame) {
        minirpc::ProtocolFrame resp = frame;
        resp.message_type = minirpc::MessageType::kResponse;
        (void)server.SendFrame(id, gen, resp, false);
    });
    assert(server.Start({"127.0.0.1", 19405}).ok());

    constexpr int kFrames = 512;
    std::mutex m;
    std::vector<uint64_t> received_ids;
    minirpc::TcpClient client;
    client.SetOnFrame([&](minirpc::ProtocolFrame frame) {
        std::lock_guard<std::mutex> lock(m);
        received_ids.push_back(frame.request_id);
        assert(frame.body.size() == 64);
    });
    assert(client.Connect({"127.0.0.1", 19405}).ok());

    for (int i = 0; i < kFrames; ++i) {
        minirpc::ProtocolFrame frame;
        frame.request_id = static_cast<uint64_t>(i + 1);
        frame.message_type = minirpc::MessageType::kRequest;
        frame.body.assign(64, static_cast<char>('a' + (i % 26)));
        assert(client.SendFrame(frame).ok());
    }

    assert(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(m);
        return received_ids.size() == kFrames;
    }, std::chrono::seconds(5)));
    {
        std::lock_guard<std::mutex> lock(m);
        for (int i = 0; i < kFrames; ++i) {
            assert(received_ids[static_cast<std::size_t>(i)] == static_cast<uint64_t>(i + 1));
        }
    }
    client.Close();
    server.Stop();
}

void TestHalfPacketFollowedByStickyPacketsPreservesOrder() {
    minirpc::TcpServer server;
    std::atomic<int> frame_count{0};
    server.SetOnFrame([&](minirpc::ConnectionId id,
                          uint64_t generation,
                          minirpc::ProtocolFrame frame) {
        frame_count.fetch_add(1);
        frame.message_type = minirpc::MessageType::kResponse;
        assert(server.SendFrame(id, generation, frame, false).ok());
    });
    assert(server.Start({"127.0.0.1", 19406}).ok());

    minirpc::ProtocolFrame first;
    first.request_id = 601;
    first.message_type = minirpc::MessageType::kRequest;
    first.body = "first-frame-arrives-in-two-parts";

    minirpc::ProtocolFrame second;
    second.request_id = 602;
    second.message_type = minirpc::MessageType::kRequest;
    second.body = "second-frame-is-sticky";

    minirpc::RpcCodec codec;
    const std::string first_wire = codec.Encode(first);
    const std::string second_wire = codec.Encode(second);
    const std::size_t split = minirpc::kProtocolHeaderSize + first.body.size() / 2;

    int fd = ConnectRaw(19406);
    SetRecvTimeout(fd, 2000);
    SendAllRaw(fd, first_wire.substr(0, split));
    SendAllRaw(fd, first_wire.substr(split) + second_wire);

    const std::vector<minirpc::ProtocolFrame> responses = ReceiveFramesRaw(fd, 2);
    assert(responses[0].request_id == first.request_id);
    assert(responses[0].body == first.body);
    assert(responses[1].request_id == second.request_id);
    assert(responses[1].body == second.body);
    assert(frame_count.load() == 2);

    ::close(fd);
    server.Stop();
}

}  // namespace

int main() {
    TestSingleEcho();
    TestLargeFrame();
    TestConcurrentClients();
    TestServerObservesClientClose();
    TestLargePayloadBurstPreservesOrder();
    TestSmallPacketHighFrequencyEchoPreservesOrder();
    TestHalfPacketFollowedByStickyPacketsPreservesOrder();
    return 0;
}
