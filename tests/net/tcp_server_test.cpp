#include <atomic>
#include <cassert>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <future>
#include <mutex>
#include <string>
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

int TryConnectRaw(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(fd != -1);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1) {
        ::close(fd);
        return -1;
    }
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

void SetReceiveBufferSize(int fd, int bytes) {
    const int rc = ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes));
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

void TestRepeatedImmediateStartStop() {
    minirpc::TcpServer server;
    for (int i = 0; i < 100; ++i) {
        assert(server.Start({"127.0.0.1", 19227}).ok());
        server.Stop();
        assert(!server.running());
    }
}

void TestConcurrentResponseWakeAndStop() {
    minirpc::TcpServer server;
    assert(server.Start({"127.0.0.1", 19229}).ok());

    minirpc::ProtocolFrame frame = MakeFrame(29, 256);
    frame.message_type = minirpc::MessageType::kResponse;
    std::atomic<bool> start{false};
    std::atomic<int> attempts{0};
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back([&] {
            while (!start.load()) {
                std::this_thread::yield();
            }
            while (server.running()) {
                (void)server.SendFrame(9999, 9999, frame, false);
                attempts.fetch_add(1);
            }
        });
    }

    start.store(true);
    assert(WaitUntil([&] { return attempts.load() >= 100; }, std::chrono::seconds(1)));
    server.Stop();
    for (auto& worker : workers) {
        worker.join();
    }
    assert(!server.running());

    assert(server.Start({"127.0.0.1", 19229}).ok());
    server.Stop();
}

void TestServerCanRestart() {
    minirpc::TcpServer server;
    std::atomic<int> open_count{0};
    std::atomic<minirpc::ConnectionId> first_id{0};
    std::atomic<uint64_t> first_generation{0};
    std::atomic<minirpc::ConnectionId> second_id{0};
    std::atomic<uint64_t> second_generation{0};
    server.SetOnOpen([&](minirpc::ConnectionId id, uint64_t generation) {
        const int index = open_count.fetch_add(1);
        if (index == 0) {
            first_id.store(id);
            first_generation.store(generation);
        } else if (index == 1) {
            second_id.store(id);
            second_generation.store(generation);
        }
    });

    assert(server.Start({"127.0.0.1", 19215}).ok());
    int first_fd = ConnectRaw(19215);
    assert(WaitUntil([&] {
        return open_count.load() == 1 && first_id.load() != 0 && first_generation.load() != 0;
    }, std::chrono::seconds(1)));
    ::close(first_fd);
    server.Stop();
    assert(!server.running());

    assert(server.Start({"127.0.0.1", 19215}).ok());
    int second_fd = ConnectRaw(19215);
    assert(WaitUntil([&] {
        return open_count.load() == 2 && second_id.load() != 0 && second_generation.load() != 0;
    }, std::chrono::seconds(1)));
    assert(second_id.load() != first_id.load());
    assert(second_generation.load() != first_generation.load());
    ::close(second_fd);
    server.Stop();
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

void TestStartCanRecoverAfterOccupiedPortFailure() {
    int squatter = OccupyPort(19216);
    minirpc::TcpServer server;
    assert(!server.Start({"127.0.0.1", 19216}).ok());
    ::close(squatter);

    assert(server.Start({"127.0.0.1", 19216}).ok());
    int fd = ConnectRaw(19216);
    ::close(fd);
    server.Stop();
}

void TestStopAcceptingKeepsExistingConnectionAlive() {
    minirpc::TcpServer server;
    std::atomic<int> open_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    server.SetOnFrame([&](minirpc::ConnectionId id,
                          uint64_t generation,
                          minirpc::ProtocolFrame frame) {
        frame.message_type = minirpc::MessageType::kResponse;
        assert(server.SendFrame(id, generation, frame, false).ok());
    });
    assert(server.Start({"127.0.0.1", 19217}).ok());

    int existing_fd = ConnectRaw(19217);
    SetRecvTimeout(existing_fd, 1000);
    assert(WaitUntil([&] { return open_count.load() == 1; }, std::chrono::seconds(1)));

    server.StopAccepting();
    assert(WaitUntil([&] {
        int fd = TryConnectRaw(19217);
        if (fd == -1) {
            return true;
        }
        ::close(fd);
        return false;
    }, std::chrono::seconds(1)));
    assert(open_count.load() == 1);

    minirpc::RpcCodec codec;
    SendAllRaw(existing_fd, codec.Encode(MakeFrame(17, 32)));
    minirpc::ProtocolFrame response = ReceiveFrameRaw(existing_fd);
    assert(response.request_id == 17);
    assert(response.message_type == minirpc::MessageType::kResponse);
    assert(response.body.size() == 32);

    ::close(existing_fd);
    server.Stop();
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

void TestCloseConnectionChecksGenerationAndAllowsWildcard() {
    minirpc::TcpServer server;
    std::atomic<minirpc::ConnectionId> conn_id{0};
    std::atomic<uint64_t> generation{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId id, uint64_t gen) {
        conn_id.store(id);
        generation.store(gen);
    });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    server.SetOnFrame([&](minirpc::ConnectionId id,
                          uint64_t gen,
                          minirpc::ProtocolFrame frame) {
        frame.message_type = minirpc::MessageType::kResponse;
        assert(server.SendFrame(id, gen, frame, false).ok());
    });
    assert(server.Start({"127.0.0.1", 19218}).ok());

    int fd = ConnectRaw(19218);
    SetRecvTimeout(fd, 1000);
    assert(WaitUntil([&] {
        return conn_id.load() != 0 && generation.load() != 0;
    }, std::chrono::seconds(1)));

    server.CloseConnection(conn_id.load(), generation.load() + 1);
    minirpc::RpcCodec codec;
    SendAllRaw(fd, codec.Encode(MakeFrame(18, 32)));
    const minirpc::ProtocolFrame response = ReceiveFrameRaw(fd);
    assert(response.request_id == 18);
    assert(close_count.load() == 0);

    server.CloseConnection(conn_id.load(), 0);
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(1)));
    AssertPeerClosed(fd);
    ::close(fd);
    server.Stop();
    assert(close_count.load() == 1);
}

void TestFrameCallbackCanCloseConnection() {
    minirpc::TcpServer server;
    std::atomic<int> frame_count{0};
    std::atomic<int> close_count{0};
    server.SetOnFrame([&](minirpc::ConnectionId id,
                          uint64_t generation,
                          minirpc::ProtocolFrame) {
        frame_count.fetch_add(1);
        server.CloseConnection(id, generation);
    });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19230}).ok());

    int fd = ConnectRaw(19230);
    SetRecvTimeout(fd, 1000);
    minirpc::RpcCodec codec;
    SendAllRaw(fd, codec.Encode(MakeFrame(30, 16)));
    assert(WaitUntil([&] {
        return frame_count.load() == 1 && close_count.load() == 1;
    }, std::chrono::seconds(1)));
    AssertPeerClosed(fd);

    ::close(fd);
    server.Stop();
    assert(close_count.load() == 1);
}

void TestPeerResetClosesConnectionOnce() {
    minirpc::TcpServer server;
    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19231}).ok());

    int fd = ConnectRaw(19231);
    assert(WaitUntil([&] { return open_count.load() == 1; }, std::chrono::seconds(1)));
    linger reset_linger{};
    reset_linger.l_onoff = 1;
    reset_linger.l_linger = 0;
    assert(::setsockopt(fd, SOL_SOCKET, SO_LINGER, &reset_linger, sizeof(reset_linger)) == 0);
    ::close(fd);

    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(1)));
    server.Stop();
    assert(close_count.load() == 1);
}

void TestResponseAndCloseQueuesDrainTogether() {
    minirpc::TcpServer server;
    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    std::atomic<minirpc::ConnectionId> first_id{0};
    std::atomic<uint64_t> first_generation{0};
    std::atomic<minirpc::ConnectionId> second_id{0};
    std::atomic<uint64_t> second_generation{0};
    server.SetOnOpen([&](minirpc::ConnectionId id, uint64_t generation) {
        if (open_count.fetch_add(1) == 0) {
            first_id.store(id);
            first_generation.store(generation);
        } else {
            second_id.store(id);
            second_generation.store(generation);
        }
    });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19232}).ok());

    int first_fd = ConnectRaw(19232);
    int second_fd = ConnectRaw(19232);
    SetRecvTimeout(first_fd, 1000);
    SetRecvTimeout(second_fd, 1000);
    assert(WaitUntil([&] {
        return open_count.load() == 2 && first_id.load() != 0 && second_id.load() != 0;
    }, std::chrono::seconds(1)));

    minirpc::ProtocolFrame response = MakeFrame(32, 64);
    response.message_type = minirpc::MessageType::kResponse;
    assert(server.SendFrame(first_id.load(), first_generation.load(), response, false).ok());
    server.CloseConnection(second_id.load(), second_generation.load());

    const minirpc::ProtocolFrame received = ReceiveFrameRaw(first_fd);
    assert(received.request_id == response.request_id);
    assert(received.body == response.body);
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(1)));
    AssertPeerClosed(second_fd);

    ::close(first_fd);
    ::close(second_fd);
    server.Stop();
    assert(close_count.load() == 2);
}

void TestProtocolErrorsCloseConnections() {
    minirpc::TcpServer server;
    std::atomic<int> open_count{0};
    std::atomic<int> frame_count{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    server.SetOnFrame([&](minirpc::ConnectionId, uint64_t, minirpc::ProtocolFrame) {
        frame_count.fetch_add(1);
    });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19219}).ok());

    minirpc::RpcCodec codec;
    std::string invalid_magic = codec.Encode(MakeFrame(19, 1));
    invalid_magic[0] = '\0';

    std::string oversized = codec.Encode(MakeFrame(20, 0));
    const uint32_t body_size =
        static_cast<uint32_t>(minirpc::kDefaultMaxFrameBodySize + 1);
    oversized[16] = static_cast<char>((body_size >> 24) & 0xff);
    oversized[17] = static_cast<char>((body_size >> 16) & 0xff);
    oversized[18] = static_cast<char>((body_size >> 8) & 0xff);
    oversized[19] = static_cast<char>(body_size & 0xff);

    const std::vector<std::string> invalid_frames = {invalid_magic, oversized};
    for (std::size_t i = 0; i < invalid_frames.size(); ++i) {
        int fd = ConnectRaw(19219);
        SetRecvTimeout(fd, 1000);
        assert(WaitUntil([&] {
            return open_count.load() == static_cast<int>(i + 1);
        }, std::chrono::seconds(1)));
        SendAllRaw(fd, invalid_frames[i]);
        assert(WaitUntil([&] {
            return close_count.load() == static_cast<int>(i + 1);
        }, std::chrono::seconds(1)));
        AssertPeerClosed(fd);
        ::close(fd);
    }

    server.Stop();
    assert(frame_count.load() == 0);
    assert(close_count.load() == 2);
}

void TestCompleteFrameBurstDoesNotTripReadBufferLimit() {
    constexpr int kFrameCount = 128;

    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.max_read_buffer_bytes = 1024;
    server.SetOptions(options);

    std::atomic<bool> open_callback_entered{false};
    std::atomic<bool> release_open_callback{false};
    std::atomic<int> frame_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) {
        open_callback_entered.store(true);
        while (!release_open_callback.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    server.SetOnFrame([&](minirpc::ConnectionId, uint64_t, minirpc::ProtocolFrame) {
        frame_count.fetch_add(1);
    });
    assert(server.Start({"127.0.0.1", 19225}).ok());

    int fd = ConnectRaw(19225);
    assert(WaitUntil([&] { return open_callback_entered.load(); }, std::chrono::seconds(1)));

    minirpc::RpcCodec codec;
    std::string burst;
    for (int i = 0; i < kFrameCount; ++i) {
        burst += codec.Encode(MakeFrame(static_cast<uint64_t>(i + 1), 32));
    }
    assert(burst.size() > options.max_read_buffer_bytes);
    SendAllRaw(fd, burst);
    release_open_callback.store(true);

    assert(WaitUntil([&] { return frame_count.load() == kFrameCount; },
                     std::chrono::seconds(2)));
    ::close(fd);
    server.Stop();
}

void TestCloseAfterSendDeliversFullFrameAndClosesOnce() {
    constexpr std::size_t kResponseBodySize = 4 * 1024 * 1024;

    minirpc::TcpServer server;
    std::atomic<int> close_count{0};
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    server.SetOnFrame([&](minirpc::ConnectionId id,
                          uint64_t generation,
                          minirpc::ProtocolFrame frame) {
        frame.message_type = minirpc::MessageType::kResponse;
        frame.body.assign(kResponseBodySize, 'r');
        assert(server.SendFrame(id, generation, frame, true).ok());
    });
    assert(server.Start({"127.0.0.1", 19220}).ok());

    int fd = ConnectRaw(19220);
    SetRecvTimeout(fd, 5000);
    minirpc::RpcCodec codec;
    SendAllRaw(fd, codec.Encode(MakeFrame(20, 8)));
    const minirpc::ProtocolFrame response = ReceiveFrameRaw(fd);
    assert(response.request_id == 20);
    assert(response.message_type == minirpc::MessageType::kResponse);
    assert(response.body.size() == kResponseBodySize);
    assert(response.body.front() == 'r');
    assert(response.body.back() == 'r');
    AssertPeerClosed(fd);
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(1)));

    ::close(fd);
    server.Stop();
    assert(close_count.load() == 1);
}

void TestResponseQueueLimitClosesConnection() {
    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.max_response_queue = 0;
    server.SetOptions(options);

    std::atomic<minirpc::ConnectionId> conn_id{0};
    std::atomic<uint64_t> generation{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId id, uint64_t gen) {
        conn_id.store(id);
        generation.store(gen);
    });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19221}).ok());

    int fd = ConnectRaw(19221);
    SetRecvTimeout(fd, 1000);
    assert(WaitUntil([&] {
        return conn_id.load() != 0 && generation.load() != 0;
    }, std::chrono::seconds(1)));

    minirpc::ProtocolFrame response = MakeFrame(21, 8);
    response.message_type = minirpc::MessageType::kResponse;
    const minirpc::Status status =
        server.SendFrame(conn_id.load(), generation.load(), response, false);
    assert(!status.ok());
    assert(status.code() == minirpc::StatusCode::kServerError);
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(1)));
    AssertPeerClosed(fd);

    ::close(fd);
    server.Stop();
    assert(close_count.load() == 1);
}

void TestConnectionChurnAndPromptStopWithIdleConnections() {
    constexpr int kBatchSize = 24;
    constexpr int kBatches = 4;

    minirpc::TcpServer server;
    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19222}).ok());

    for (int batch = 0; batch < kBatches; ++batch) {
        std::vector<int> fds;
        fds.reserve(kBatchSize);
        for (int i = 0; i < kBatchSize; ++i) {
            fds.push_back(ConnectRaw(19222));
        }
        const int expected = (batch + 1) * kBatchSize;
        assert(WaitUntil([&] { return open_count.load() == expected; }, std::chrono::seconds(2)));
        for (int fd : fds) {
            ::close(fd);
        }
        assert(WaitUntil([&] { return close_count.load() == expected; }, std::chrono::seconds(2)));
    }

    std::vector<int> idle_fds;
    idle_fds.reserve(kBatchSize);
    for (int i = 0; i < kBatchSize; ++i) {
        idle_fds.push_back(ConnectRaw(19222));
    }
    const int total_connections = (kBatches + 1) * kBatchSize;
    assert(WaitUntil([&] {
        return open_count.load() == total_connections;
    }, std::chrono::seconds(2)));

    const auto stop_start = std::chrono::steady_clock::now();
    server.Stop();
    const auto stop_elapsed = std::chrono::steady_clock::now() - stop_start;
    assert(stop_elapsed < std::chrono::seconds(2));
    assert(close_count.load() == total_connections);

    for (int fd : idle_fds) {
        ::close(fd);
    }
}

void TestStopCancelsInFlightSend() {
    constexpr std::size_t kResponseBodySize = 8 * 1024 * 1024;

    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.high_watermark_bytes = 1;
    options.low_watermark_bytes = 0;
    options.max_write_buffer_bytes = 32 * 1024 * 1024;
    server.SetOptions(options);

    std::atomic<bool> in_backpressure{false};
    std::atomic<int> close_count{0};
    server.SetOnBackpressure([&](minirpc::ConnectionId,
                                 uint64_t,
                                 int,
                                 bool enabled,
                                 std::size_t) {
        if (enabled) {
            in_backpressure.store(true);
        }
    });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    server.SetOnFrame([&](minirpc::ConnectionId id,
                          uint64_t generation,
                          minirpc::ProtocolFrame frame) {
        frame.message_type = minirpc::MessageType::kResponse;
        frame.body.assign(kResponseBodySize, 's');
        assert(server.SendFrame(id, generation, frame, false).ok());
    });
    assert(server.Start({"127.0.0.1", 19223}).ok());

    int fd = ConnectRaw(19223);
    SetReceiveBufferSize(fd, 4096);
    minirpc::RpcCodec codec;
    SendAllRaw(fd, codec.Encode(MakeFrame(23, 8)));
    assert(WaitUntil([&] { return in_backpressure.load(); }, std::chrono::seconds(3)));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    const auto stop_start = std::chrono::steady_clock::now();
    server.Stop();
    const auto stop_elapsed = std::chrono::steady_clock::now() - stop_start;
    assert(stop_elapsed < std::chrono::seconds(2));
    assert(close_count.load() == 1);

    ::close(fd);
}

void TestWriteBufferHardLimitOnlyClosesSlowConnection() {
    constexpr std::size_t kSlowResponseBodySize = 8 * 1024 * 1024;

    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.high_watermark_bytes = 16 * 1024;
    options.low_watermark_bytes = 4 * 1024;
    options.max_write_buffer_bytes = 64 * 1024;
    server.SetOptions(options);

    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    server.SetOnFrame([&](minirpc::ConnectionId id,
                          uint64_t generation,
                          minirpc::ProtocolFrame frame) {
        frame.message_type = minirpc::MessageType::kResponse;
        frame.body.assign(frame.request_id == 1 ? kSlowResponseBodySize : 32, 'h');
        assert(server.SendFrame(id, generation, frame, false).ok());
    });
    assert(server.Start({"127.0.0.1", 19233}).ok());

    minirpc::RpcCodec codec;
    int slow_fd = ConnectRaw(19233);
    SetReceiveBufferSize(slow_fd, 4096);
    SendAllRaw(slow_fd, codec.Encode(MakeFrame(1, 8)));
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(3)));

    int normal_fd = ConnectRaw(19233);
    SetRecvTimeout(normal_fd, 2000);
    assert(WaitUntil([&] { return open_count.load() == 2; }, std::chrono::seconds(1)));
    SendAllRaw(normal_fd, codec.Encode(MakeFrame(2, 8)));
    const minirpc::ProtocolFrame response = ReceiveFrameRaw(normal_fd);
    assert(response.request_id == 2);
    assert(response.body.size() == 32);
    assert(close_count.load() == 1);

    ::close(slow_fd);
    ::close(normal_fd);
    server.Stop();
    assert(close_count.load() == 2);
}

void TestStopWithMoreIdleConnectionsThanRingDepth() {
    constexpr int kConnectionCount = 300;

    minirpc::TcpServer server;
    minirpc::TcpServerOptions options;
    options.backlog = 512;
    server.SetOptions(options);
    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    assert(server.Start({"127.0.0.1", 19234}).ok());

    std::vector<int> fds;
    fds.reserve(kConnectionCount);
    for (int i = 0; i < kConnectionCount; ++i) {
        fds.push_back(ConnectRaw(19234));
    }
    assert(WaitUntil([&] { return open_count.load() == kConnectionCount; },
                     std::chrono::seconds(5)));

    std::future<void> stop = std::async(std::launch::async, [&] { server.Stop(); });
    assert(stop.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
    stop.get();
    assert(close_count.load() == kConnectionCount);

    for (int fd : fds) {
        ::close(fd);
    }
}

void TestDelayedWorkerResponseAfterPeerCloseIsDropped() {
    minirpc::TcpServer server;
    std::atomic<bool> handler_started{false};
    std::atomic<bool> worker_send_ok{false};
    std::atomic<int> close_count{0};
    std::thread delayed_worker;

    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) { close_count.fetch_add(1); });
    server.SetOnFrame([&](minirpc::ConnectionId id,
                          uint64_t generation,
                          minirpc::ProtocolFrame frame) {
        delayed_worker = std::thread([&, id, generation, frame = std::move(frame)]() mutable {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            frame.message_type = minirpc::MessageType::kResponse;
            worker_send_ok.store(server.SendFrame(id, generation, frame, false).ok());
        });
        handler_started.store(true);
    });
    assert(server.Start({"127.0.0.1", 19224}).ok());

    int fd = ConnectRaw(19224);
    minirpc::RpcCodec codec;
    SendAllRaw(fd, codec.Encode(MakeFrame(24, 8)));
    assert(WaitUntil([&] { return handler_started.load(); }, std::chrono::seconds(1)));
    ::close(fd);
    assert(WaitUntil([&] { return close_count.load() == 1; }, std::chrono::seconds(1)));

    delayed_worker.join();
    assert(worker_send_ok.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    server.Stop();
    assert(close_count.load() == 1);
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

void TestCallbacksStayOnReactorThreadDuringStop() {
    minirpc::TcpServer server;
    const std::thread::id caller_thread = std::this_thread::get_id();
    std::thread::id open_thread;
    std::thread::id frame_thread;
    std::thread::id close_thread;
    std::atomic<bool> opened{false};
    std::atomic<bool> frame_seen{false};
    std::atomic<bool> closed{false};

    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) {
        open_thread = std::this_thread::get_id();
        opened.store(true);
    });
    server.SetOnFrame([&](minirpc::ConnectionId, uint64_t, minirpc::ProtocolFrame) {
        frame_thread = std::this_thread::get_id();
        frame_seen.store(true);
    });
    server.SetOnClose([&](minirpc::ConnectionId, uint64_t) {
        close_thread = std::this_thread::get_id();
        closed.store(true);
    });
    assert(server.Start({"127.0.0.1", 19226}).ok());

    int fd = ConnectRaw(19226);
    assert(WaitUntil([&] { return opened.load(); }, std::chrono::seconds(1)));
    minirpc::RpcCodec codec;
    SendAllRaw(fd, codec.Encode(MakeFrame(26, 8)));
    assert(WaitUntil([&] { return frame_seen.load(); }, std::chrono::seconds(1)));

    server.Stop();
    assert(closed.load());
    assert(open_thread == frame_thread);
    assert(open_thread == close_thread);
    assert(open_thread != caller_thread);
    ::close(fd);
}

void TestStopFromFrameCallbackCanRestart() {
    minirpc::TcpServer server;
    std::atomic<int> frame_count{0};
    server.SetOnFrame([&](minirpc::ConnectionId, uint64_t, minirpc::ProtocolFrame) {
        if (frame_count.fetch_add(1) == 0) {
            server.Stop();
        }
    });
    assert(server.Start({"127.0.0.1", 19228}).ok());

    minirpc::RpcCodec codec;
    int first_fd = ConnectRaw(19228);
    SendAllRaw(first_fd, codec.Encode(MakeFrame(28, 8)));
    assert(WaitUntil([&] {
        return frame_count.load() == 1 && !server.running();
    }, std::chrono::seconds(1)));
    ::close(first_fd);

    assert(server.Start({"127.0.0.1", 19228}).ok());
    int second_fd = ConnectRaw(19228);
    ::close(second_fd);
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
    std::atomic<int> open_count{0};
    std::atomic<bool> normal_request_seen{false};
    std::atomic<std::size_t> max_seen_buffer{0};
    server.SetOnOpen([&](minirpc::ConnectionId, uint64_t) { open_count.fetch_add(1); });
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
        if (frame.request_id == 10000) {
            normal_request_seen.store(true);
        }
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
    SetRecvTimeout(normal_fd, 3000);
    assert(WaitUntil([&] { return open_count.load() == 2; }, std::chrono::seconds(2)));
    SendAllRaw(normal_fd, codec.Encode(MakeFrame(10000, 8)));
    assert(WaitUntil([&] { return normal_request_seen.load(); }, std::chrono::seconds(2)));
    const minirpc::ProtocolFrame normal_response = ReceiveFrameRaw(normal_fd);
    assert(normal_response.request_id == 10000);
    assert(normal_response.message_type == minirpc::MessageType::kResponse);
    assert(normal_response.body.size() == 128 * 1024);
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
    TestRepeatedImmediateStartStop();
    TestConcurrentResponseWakeAndStop();
    TestServerCanRestart();
    TestStartFailsOnOccupiedPort();
    TestStartCanRecoverAfterOccupiedPortFailure();
    TestStopAcceptingKeepsExistingConnectionAlive();
    TestSendFrameOnStaleGenerationIsDropped();
    TestCloseConnectionChecksGenerationAndAllowsWildcard();
    TestFrameCallbackCanCloseConnection();
    TestPeerResetClosesConnectionOnce();
    TestResponseAndCloseQueuesDrainTogether();
    TestProtocolErrorsCloseConnections();
    TestCompleteFrameBurstDoesNotTripReadBufferLimit();
    TestCloseAfterSendDeliversFullFrameAndClosesOnce();
    TestResponseQueueLimitClosesConnection();
    TestConnectionChurnAndPromptStopWithIdleConnections();
    TestStopCancelsInFlightSend();
    TestWriteBufferHardLimitOnlyClosesSlowConnection();
    TestStopWithMoreIdleConnectionsThanRingDepth();
    TestDelayedWorkerResponseAfterPeerCloseIsDropped();
    TestCloseConnectionFiresOnCloseExactlyOnce();
    TestCallbacksStayOnReactorThreadDuringStop();
    TestStopFromFrameCallbackCanRestart();
    TestBackpressurePausesSlowConnectionAndOtherClientsContinue();
    return 0;
}
