#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/net/tcp_client.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"

namespace {

#ifdef __linux__

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

#endif  // __linux__

}  // namespace

int main() {
#ifdef __linux__
    TestConnectFailureOnDeadEndpoint();
    TestConnectSuccessThenIdempotent();
    TestPeerClosedFiresOnClose();
    TestSendAfterCloseFails();
#endif
    return 0;
}
