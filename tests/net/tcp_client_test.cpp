#include <atomic>
#include <cassert>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <future>
#include <limits>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/net/tcp_client.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"

namespace {

struct TinyListener {
    int listen_fd = -1;
    int accepted_fd = -1;
    std::thread thread;

    void Start(uint16_t port) {
        listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int enabled = 1;
        ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        int rc = ::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        assert(rc == 0);
        rc = ::listen(listen_fd, 4);
        assert(rc == 0);
        thread = std::thread([this] {
            sockaddr_in c{};
            socklen_t len = sizeof(c);
            accepted_fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&c), &len);
        });
    }

    void Stop() {
        if (accepted_fd != -1) ::close(accepted_fd);
        if (listen_fd != -1) ::close(listen_fd);
        if (thread.joinable()) thread.join();
    }
};

void SendAllRaw(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n == -1 && errno == EINTR) continue;
        assert(n > 0);
        sent += static_cast<std::size_t>(n);
    }
}

void TestConnectFailureOnDeadEndpoint() {
    minirpc::TcpClient client;
    auto status = client.Connect({"127.0.0.1", 1});
    assert(!status.ok());
    assert(status.code() == minirpc::StatusCode::kNetworkError);
    assert(client.state() == minirpc::ConnectionState::kDisconnected);
}

void TestConnectSuccessThenIdempotent() {
    TinyListener listener;
    listener.Start(19310);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    minirpc::TcpClient client;
    auto s1 = client.Connect({"127.0.0.1", 19310});
    assert(s1.ok());
    assert(client.state() == minirpc::ConnectionState::kConnected);
    auto s2 = client.Connect({"127.0.0.1", 19310});
    assert(s2.ok());

    client.Close();
    listener.Stop();
}

void TestPeerClosedFiresOnClose() {
    TinyListener listener;
    listener.Start(19311);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::atomic<int> close_count{0};
    std::atomic<int> reason_code{-1};
    minirpc::TcpClient client;
    client.SetOnClose([&](minirpc::TcpClient::CloseReason r, const std::string&) {
        close_count.fetch_add(1);
        reason_code.store(static_cast<int>(r));
    });
    auto status = client.Connect({"127.0.0.1", 19311});
    assert(status.ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    listener.Stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    assert(close_count.load() == 1);
    assert(reason_code.load() == static_cast<int>(minirpc::TcpClient::CloseReason::kPeerClosed));
}

void TestSendAfterCloseFails() {
    TinyListener listener;
    listener.Start(19312);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    minirpc::TcpClient client;
    auto status = client.Connect({"127.0.0.1", 19312});
    assert(status.ok());
    client.Close();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    minirpc::ProtocolFrame frame;
    frame.request_id = 1;
    frame.body = "x";
    auto send_status = client.SendFrame(frame);
    assert(!send_status.ok());
    assert(send_status.code() == minirpc::StatusCode::kNetworkError);

    listener.Stop();
}

void TestDefaultReadLimitRejectsOversizedHeader() {
    TinyListener listener;
    listener.Start(19313);
    std::promise<minirpc::TcpClient::CloseReason> closed;
    auto close_future = closed.get_future();
    std::atomic<int> close_count{0};
    std::atomic<int> frame_count{0};
    minirpc::TcpClient client;
    client.SetOnFrame([&](minirpc::ProtocolFrame) { frame_count.fetch_add(1); });
    client.SetOnClose([&](minirpc::TcpClient::CloseReason reason, const std::string& message) {
        assert(!message.empty());
        assert(close_count.fetch_add(1) == 0);
        closed.set_value(reason);
    });
    assert(client.Connect({"127.0.0.1", 19313}).ok());
    listener.thread.join();

    minirpc::ProtocolFrame frame;
    frame.message_type = minirpc::MessageType::kResponse;
    frame.body.assign(2 * 1024 * 1024, 'x');
    minirpc::RpcCodec codec;
    std::string header = codec.Encode(frame);
    header.resize(minirpc::kProtocolHeaderSize);
    assert(::send(listener.accepted_fd, header.data(), header.size(), MSG_NOSIGNAL) ==
           static_cast<ssize_t>(header.size()));
    assert(close_future.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    assert(close_future.get() == minirpc::TcpClient::CloseReason::kProtocolError);
    assert(client.state() == minirpc::ConnectionState::kDisconnected);
    client.Close();
    listener.Stop();
    assert(close_count.load() == 1);
    assert(frame_count.load() == 0);
}

void TestInvalidReadLimitFailsBeforeConnecting() {
    for (const std::size_t limit : {std::size_t{0}, minirpc::kProtocolHeaderSize - 1}) {
        minirpc::TcpClientOptions options;
        options.max_read_buffer_bytes = limit;
        minirpc::TcpClient client(options);
        const auto status = client.Connect({"127.0.0.1", 1});
        assert(!status.ok());
        assert(status.code() == minirpc::StatusCode::kNetworkError);
        assert(status.message().find("max_read_buffer_bytes") != std::string::npos);
        assert(client.state() == minirpc::ConnectionState::kDisconnected);
    }
}

void TestConfiguredReadLimitAllowsFragmentedAndCoalescedFrames() {
    const std::size_t limits[] = {
        minirpc::kProtocolHeaderSize, 1024, 2 * 1024 * 1024, 3 * 1024 * 1024};
    for (const std::size_t limit : limits) {
        TinyListener listener;
        listener.Start(19314);
        minirpc::TcpClientOptions options;
        options.max_read_buffer_bytes = limit;
        minirpc::TcpClient client(options);
        std::promise<void> received;
        auto received_future = received.get_future();
        int frame_count = 0;
        client.SetOnFrame([&](minirpc::ProtocolFrame frame) {
            ++frame_count;
            assert(frame.request_id == static_cast<uint64_t>(frame_count));
            const std::size_t expected_size = frame_count == 1
                ? limit - minirpc::kProtocolHeaderSize : 0;
            assert(frame.body == std::string(expected_size, 'x'));
            if (frame_count == 3) received.set_value();
        });
        assert(client.Connect({"127.0.0.1", 19314}).ok());
        listener.thread.join();
        minirpc::RpcCodec codec;
        minirpc::ProtocolFrame frame;
        frame.message_type = minirpc::MessageType::kResponse;
        frame.request_id = 1;
        frame.body.assign(limit - minirpc::kProtocolHeaderSize, 'x');
        std::string burst = codec.Encode(frame);
        frame.body.clear();
        frame.request_id = 2;
        burst += codec.Encode(frame);
        frame.request_id = 3;
        burst += codec.Encode(frame);
        assert(burst.size() > limit);
        SendAllRaw(listener.accepted_fd, burst.substr(0, minirpc::kProtocolHeaderSize - 1));
        SendAllRaw(listener.accepted_fd, burst.substr(minirpc::kProtocolHeaderSize - 1));
        assert(received_future.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
        assert(client.state() == minirpc::ConnectionState::kConnected);
        client.Close();
        listener.Stop();
        assert(frame_count == 3);
    }
}

void TestConfiguredReadLimitRejectsOversizedFrameAndCanReconnect() {
    const std::size_t limits[] = {1024, std::numeric_limits<std::size_t>::max()};
    for (const std::size_t limit : limits) {
        TinyListener listener;
        listener.Start(19315);
        minirpc::TcpClientOptions options;
        options.max_read_buffer_bytes = limit;
        minirpc::TcpClient client(options);
        std::promise<minirpc::TcpClient::CloseReason> closed;
        auto close_future = closed.get_future();
        client.SetOnClose([&](minirpc::TcpClient::CloseReason reason, const std::string& message) {
            assert(!message.empty());
            closed.set_value(reason);
        });
        assert(client.Connect({"127.0.0.1", 19315}).ok());
        listener.thread.join();
        minirpc::RpcCodec codec;
        minirpc::ProtocolFrame frame;
        frame.body.assign(limit == 1024 ? limit - minirpc::kProtocolHeaderSize + 1
                                       : minirpc::kDefaultMaxFrameBodySize + 1, 'x');
        std::string header = codec.Encode(frame);
        header.resize(minirpc::kProtocolHeaderSize);
        SendAllRaw(listener.accepted_fd, header);
        assert(close_future.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
        assert(close_future.get() == minirpc::TcpClient::CloseReason::kProtocolError);
        client.Close();
        listener.Stop();

        TinyListener next_listener;
        next_listener.Start(19315);
        client.SetOnClose({});
        std::promise<void> received;
        auto received_future = received.get_future();
        client.SetOnFrame([&](minirpc::ProtocolFrame decoded) {
            assert(decoded.body == "reconnected");
            received.set_value();
        });
        assert(client.Connect({"127.0.0.1", 19315}).ok());
        next_listener.thread.join();
        frame.body = "reconnected";
        SendAllRaw(next_listener.accepted_fd, codec.Encode(frame));
        assert(received_future.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
        client.Close();
        next_listener.Stop();
    }
}

}  // namespace

int main() {
    TestDefaultReadLimitRejectsOversizedHeader();
    TestInvalidReadLimitFailsBeforeConnecting();
    TestConfiguredReadLimitAllowsFragmentedAndCoalescedFrames();
    TestConfiguredReadLimitRejectsOversizedFrameAndCanReconnect();
    TestConnectFailureOnDeadEndpoint();
    TestConnectSuccessThenIdempotent();
    TestPeerClosedFiresOnClose();
    TestSendAfterCloseFails();
    return 0;
}
