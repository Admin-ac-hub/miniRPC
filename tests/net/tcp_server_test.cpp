#include <cassert>
#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
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

}  // namespace

int main() {
    TestStartStopIdempotency();
    TestStartFailsOnOccupiedPort();
    TestSendFrameOnStaleGenerationIsDropped();
    TestCloseConnectionFiresOnCloseExactlyOnce();
    return 0;
}
