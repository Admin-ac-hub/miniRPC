#include <atomic>
#include <cassert>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "minirpc/net/tcp_server.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"

namespace {

int OccupyPort(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(fd != -1);
    int enabled = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    int rc = ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    assert(rc == 0);
    rc = ::listen(fd, 4);
    assert(rc == 0);
    return fd;
}

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

void TestStartStopIdempotency() {
    minirpc::TcpServer server;
    auto s1 = server.Start({"127.0.0.1", 19210});
    assert(s1.ok());
    auto s2 = server.Start({"127.0.0.1", 19210});
    assert(s2.ok());  // second Start is a no-op while running
    server.Stop();
    server.Stop();    // double Stop is safe
    assert(!server.running());
}

void TestStartFailsOnOccupiedPort() {
    int squatter = OccupyPort(19211);
    minirpc::TcpServer server;
    auto status = server.Start({"127.0.0.1", 19211});
    assert(!status.ok());
    assert(status.code() == minirpc::StatusCode::kNetworkError);
    ::close(squatter);
}

void TestSendFrameOnStaleGenerationIsDropped() {
    minirpc::TcpServer server;
    std::mutex m;
    std::vector<minirpc::ConnectionId> seen;
    server.SetOnFrame([&](minirpc::ConnectionId id, uint64_t gen, minirpc::ProtocolFrame frame) {
        std::lock_guard<std::mutex> lock(m);
        seen.push_back(id);
        // Echo back to keep the codec happy.
        minirpc::ProtocolFrame resp = frame;
        resp.message_type = minirpc::MessageType::kResponse;
        (void)server.SendFrame(id, gen, resp, false);
    });
    auto status = server.Start({"127.0.0.1", 19212});
    assert(status.ok());

    // Stale id 9999 / gen 9999: must be silently dropped, not crash.
    minirpc::ProtocolFrame frame;
    frame.request_id = 1;
    frame.message_type = minirpc::MessageType::kResponse;
    auto send_status = server.SendFrame(9999, 9999, frame, false);
    assert(send_status.ok());  // enqueued; reactor drops it on drain
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    server.Stop();
}

void TestCloseConnectionFiresOnCloseExactlyOnce() {
    minirpc::TcpServer server;
    std::atomic<int> close_count{0};
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    auto status = server.Start({"127.0.0.1", 19213});
    assert(status.ok());

    // Open a client socket so the server has a real connection.
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(19213);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    assert(rc == 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    ::close(fd);  // peer-close triggers OnClose
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(close_count.load() == 1);

    server.Stop();
}

void TestBackpressurePausesSlowConnectionAndOtherClientsContinue() {
    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.high_watermark_bytes = 64 * 1024;
    options.low_watermark_bytes = 16 * 1024;
    options.max_write_buffer_bytes = 64 * 1024 * 1024;
    server.SetOptions(options);

    std::atomic<int> enter_count{0};
    std::atomic<int> leave_count{0};
    std::atomic<std::size_t> max_seen_buffer{0};
    server.SetOnBackpressure([&](minirpc::ConnectionId,
                                 uint64_t,
                                 int,
                                 bool in_backpressure,
                                 std::size_t write_buffer_size) {
        if (in_backpressure) {
            enter_count.fetch_add(1);
        } else {
            leave_count.fetch_add(1);
        }
        std::size_t observed = max_seen_buffer.load();
        while (write_buffer_size > observed &&
               !max_seen_buffer.compare_exchange_weak(observed, write_buffer_size)) {
        }
    });
    server.SetOnFrame([&](minirpc::ConnectionId id, uint64_t gen, minirpc::ProtocolFrame frame) {
        minirpc::ProtocolFrame response = frame;
        response.message_type = minirpc::MessageType::kResponse;
        response.body.assign(128 * 1024, 'r');
        (void)server.SendFrame(id, gen, response, false);
    });
    assert(server.Start({"127.0.0.1", 19214}).ok());

    minirpc::RpcCodec codec;
    int slow_fd = ConnectRaw(19214);
    SetRecvTimeout(slow_fd, 200);
    for (uint64_t i = 0; i < 128; ++i) {
        SendAllRaw(slow_fd, codec.Encode(MakeFrame(i + 1, 16)));
    }

    assert(WaitUntil([&] { return enter_count.load() > 0; }, std::chrono::seconds(3)));
    assert(max_seen_buffer.load() >= options.high_watermark_bytes);

    int normal_fd = ConnectRaw(19214);
    SetRecvTimeout(normal_fd, 1000);
    SendAllRaw(normal_fd, codec.Encode(MakeFrame(10000, 8)));
    std::string normal_buffer;
    normal_buffer.resize(256 * 1024);
    const ssize_t normal_read = ::recv(normal_fd, normal_buffer.data(), normal_buffer.size(), 0);
    assert(normal_read > 0);
    ::close(normal_fd);

    std::vector<char> drain(256 * 1024);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline && leave_count.load() == 0) {
        const ssize_t n = ::recv(slow_fd, drain.data(), drain.size(), 0);
        if (n == 0) {
            break;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
    }
    assert(leave_count.load() > 0);

    ::close(slow_fd);
    server.Stop();
}

}  // namespace

int main() {
    TestStartStopIdempotency();
    TestStartFailsOnOccupiedPort();
    TestSendFrameOnStaleGenerationIsDropped();
    TestCloseConnectionFiresOnCloseExactlyOnce();
    TestBackpressurePausesSlowConnectionAndOtherClientsContinue();
    return 0;
}
