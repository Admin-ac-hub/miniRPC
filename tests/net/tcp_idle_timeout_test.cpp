#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <functional>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/net/tcp_server.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"

namespace {

int ConnectRaw(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(fd != -1);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    assert(rc == 0);
    return fd;
}

void SendAllRaw(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
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

minirpc::ProtocolFrame MakeFrame(uint64_t request_id, std::size_t body_size) {
    minirpc::ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = minirpc::MessageType::kRequest;
    frame.codec_type = minirpc::CodecType::kRaw;
    frame.body.assign(body_size, 'x');
    return frame;
}

minirpc::ProtocolFrame ReceiveFrameRaw(int fd) {
    minirpc::RpcCodec codec;
    std::string buffer;
    while (true) {
        char chunk[4096];
        ssize_t n = 0;
        do {
            n = ::recv(fd, chunk, sizeof(chunk), 0);
        } while (n == -1 && errno == EINTR);
        if (n <= 0) {
            std::fprintf(stderr,
                         "ReceiveFrameRaw failed: n=%zd errno=%d buffered=%zu\n",
                         n,
                         errno,
                         buffer.size());
        }
        assert(n > 0);
        buffer.append(chunk, static_cast<std::size_t>(n));

        minirpc::ProtocolFrame frame;
        std::string error;
        const minirpc::DecodeResult result = codec.TryDecode(buffer, &frame, &error);
        assert(result != minirpc::DecodeResult::kProtocolError);
        if (result == minirpc::DecodeResult::kSuccess) {
            return frame;
        }
    }
}

void AssertPeerClosed(int fd) {
    char byte = 0;
    ssize_t n = 0;
    do {
        n = ::recv(fd, &byte, sizeof(byte), 0);
    } while (n == -1 && errno == EINTR);
    assert(n == 0 ||
           (n == -1 && (errno == ECONNRESET || errno == ENOTCONN)));
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

void SetEchoHandler(minirpc::TcpServer* server, std::atomic<int>* close_count) {
    server->SetOnClose(
        [close_count](minirpc::ConnectionId, uint64_t) { close_count->fetch_add(1); });
    server->SetOnFrame([server](minirpc::ConnectionId id,
                                uint64_t generation,
                                minirpc::ProtocolFrame frame) {
        frame.message_type = minirpc::MessageType::kResponse;
        assert(server->SendFrame(id, generation, frame, false).ok());
    });
}

void TestIdleConnectionClosed() {
    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.idle_timeout_ms = std::chrono::milliseconds(200);
    server.SetOptions(options);

    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19240}).ok());

    int fd = ConnectRaw(19240);
    SetRecvTimeout(fd, 3000);
    assert(WaitUntil([&] { return open_count.load() == 1; }, std::chrono::seconds(1)));

    const auto start = std::chrono::steady_clock::now();
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(3)));
    const auto elapsed = std::chrono::steady_clock::now() - start;
    assert(elapsed >= std::chrono::milliseconds(150));
    assert(elapsed < std::chrono::seconds(1));
    AssertPeerClosed(fd);

    ::close(fd);
    server.Stop();
}

void TestActiveConnectionSurvives() {
    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.idle_timeout_ms = std::chrono::milliseconds(200);
    server.SetOptions(options);

    std::atomic<int> close_count{0};
    SetEchoHandler(&server, &close_count);
    assert(server.Start({"127.0.0.1", 19241}).ok());

    int fd = ConnectRaw(19241);
    SetRecvTimeout(fd, 2000);
    minirpc::RpcCodec codec;

    // 持续 1s（> 4x 超时），每 50ms 一个请求：持续活跃的连接不得被关闭。
    for (int i = 0; i < 20; ++i) {
        SendAllRaw(fd, codec.Encode(MakeFrame(static_cast<uint64_t>(i + 1), 32)));
        const minirpc::ProtocolFrame response = ReceiveFrameRaw(fd);
        assert(response.request_id == static_cast<uint64_t>(i + 1));
        assert(response.message_type == minirpc::MessageType::kResponse);
        assert(response.body.size() == 32);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    assert(close_count.load() == 0);

    ::close(fd);
    server.Stop();
}

void TestHalfPacketConnectionClosed() {
    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.idle_timeout_ms = std::chrono::milliseconds(200);
    server.SetOptions(options);

    std::atomic<int> close_count{0};
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19242}).ok());

    minirpc::RpcCodec codec;
    // 合法帧的前 500 字节：头部声明 1024 body，只发一半 → 只能 kNeedMoreData，
    // 不触发协议错误，也不触发 max_read_buffer_bytes。
    std::string half = codec.Encode(MakeFrame(42, 1024));
    assert(half.size() > 500);
    half.resize(500);

    int fd = ConnectRaw(19242);
    SetRecvTimeout(fd, 3000);
    SendAllRaw(fd, half);
    // 装死：不发剩余数据也不关连接 → 必须被空闲超时回收。
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(3)));
    AssertPeerClosed(fd);

    ::close(fd);
    server.Stop();
}

void TestDisabledByDefault() {
    minirpc::TcpServer server;  // 不设置 idle_timeout_ms：默认 0 = 禁用。
    std::atomic<int> close_count{0};
    SetEchoHandler(&server, &close_count);
    assert(server.Start({"127.0.0.1", 19243}).ok());

    int fd = ConnectRaw(19243);
    SetRecvTimeout(fd, 2000);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    assert(close_count.load() == 0);

    // 静置 1s 后连接仍完全可用：向后兼容回归。
    minirpc::RpcCodec codec;
    SendAllRaw(fd, codec.Encode(MakeFrame(43, 16)));
    const minirpc::ProtocolFrame response = ReceiveFrameRaw(fd);
    assert(response.request_id == 43);
    assert(close_count.load() == 0);

    ::close(fd);
    server.Stop();
}

void TestCapacityReleasedAfterIdleClose() {
    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.max_connections = 2;
    options.idle_timeout_ms = std::chrono::milliseconds(200);
    server.SetOptions(options);

    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    SetEchoHandler(&server, &close_count);
    assert(server.Start({"127.0.0.1", 19244}).ok());

    int first_fd = ConnectRaw(19244);
    int second_fd = ConnectRaw(19244);
    assert(WaitUntil([&] { return open_count.load() == 2; }, std::chrono::seconds(1)));

    // 两条连接空闲超时被关 → 配额必须立即释放。
    assert(WaitUntil([&] { return close_count.load() == 2; }, std::chrono::seconds(3)));

    int third_fd = ConnectRaw(19244);
    SetRecvTimeout(third_fd, 2000);
    assert(WaitUntil([&] { return open_count.load() == 3; }, std::chrono::seconds(1)));
    minirpc::RpcCodec codec;
    SendAllRaw(third_fd, codec.Encode(MakeFrame(44, 16)));
    const minirpc::ProtocolFrame response = ReceiveFrameRaw(third_fd);
    assert(response.request_id == 44);

    ::close(first_fd);
    ::close(second_fd);
    ::close(third_fd);
    server.Stop();
}

void TestStaleTimerDoesNotCloseRenewedConnection() {
    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.idle_timeout_ms = std::chrono::milliseconds(500);
    server.SetOptions(options);

    std::atomic<int> close_count{0};
    SetEchoHandler(&server, &close_count);
    assert(server.Start({"127.0.0.1", 19245}).ok());

    int fd = ConnectRaw(19245);
    SetRecvTimeout(fd, 3000);
    minirpc::RpcCodec codec;

    // t0: 请求 1（定时器项 deadline = t0 + 500）。
    SendAllRaw(fd, codec.Encode(MakeFrame(1, 16)));
    assert(ReceiveFrameRaw(fd).request_id == 1);

    // t0+300: 请求 2 活跃（activity_seq 前进，但堆里仍是 t0 的旧 deadline）。
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    SendAllRaw(fd, codec.Encode(MakeFrame(2, 16)));
    assert(ReceiveFrameRaw(fd).request_id == 2);

    // 跨过旧 deadline（t0+500）之后连接必须仍存活：
    // 错误实现会拿过期定时器关掉这条已续期的连接。
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    SendAllRaw(fd, codec.Encode(MakeFrame(3, 16)));
    assert(ReceiveFrameRaw(fd).request_id == 3);

    // 停止活跃后按最后一次活跃时间被真正超时关闭。
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(3)));
    AssertPeerClosed(fd);

    ::close(fd);
    server.Stop();
}

void TestClosedConnectionTimerDoesNotAffectNewConnection() {
    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.idle_timeout_ms = std::chrono::milliseconds(300);
    server.SetOptions(options);

    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    SetEchoHandler(&server, &close_count);
    assert(server.Start({"127.0.0.1", 19246}).ok());

    minirpc::RpcCodec codec;

    // 第一条连接：产生 pending 定时器项后立刻被对端关闭。
    int first_fd = ConnectRaw(19246);
    SetRecvTimeout(first_fd, 1000);
    assert(WaitUntil([&] { return open_count.load() == 1; }, std::chrono::seconds(1)));
    SendAllRaw(first_fd, codec.Encode(MakeFrame(1, 16)));
    assert(ReceiveFrameRaw(first_fd).request_id == 1);
    ::close(first_fd);
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(1)));

    // 第二条连接：持续活跃跨过第一条的 deadline，残留定时器不得误伤它。
    int second_fd = ConnectRaw(19246);
    SetRecvTimeout(second_fd, 2000);
    assert(WaitUntil([&] { return open_count.load() == 2; }, std::chrono::seconds(1)));
    for (int i = 0; i < 6; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        SendAllRaw(second_fd, codec.Encode(MakeFrame(static_cast<uint64_t>(i + 2), 16)));
        assert(ReceiveFrameRaw(second_fd).request_id == static_cast<uint64_t>(i + 2));
    }
    assert(close_count.load() == 1);

    // 第二条连接停止活跃后按自身超时关闭。
    assert(WaitUntil([&] { return close_count.load() == 2; }, std::chrono::seconds(3)));
    AssertPeerClosed(second_fd);

    ::close(second_fd);
    server.Stop();
}

}  // namespace

int main() {
    TestIdleConnectionClosed();
    TestActiveConnectionSurvives();
    TestHalfPacketConnectionClosed();
    TestDisabledByDefault();
    TestCapacityReleasedAfterIdleClose();
    TestStaleTimerDoesNotCloseRenewedConnection();
    TestClosedConnectionTimerDoesNotAffectNewConnection();
    return 0;
}
