#include "minirpc/net/tcp_server_backend.h"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "minirpc/net/socket_utils.h"
#include "minirpc/observability/logger.h"
#include "minirpc/protocol/codec.h"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace minirpc {
namespace {

constexpr int kMaxEvents = 64;
constexpr std::size_t kReadChunkSize = 4096;
thread_local const void* current_epoll_reactor = nullptr;

void CloseFd(int* fd) {
    if (*fd != -1) {
        ::close(*fd);
        *fd = -1;
    }
}

void CompleteWrite(std::function<void(Status)>* completion, Status status) {
    if (!*completion) {
        return;
    }
    std::function<void(Status)> callback = std::move(*completion);
    try {
        callback(std::move(status));
    } catch (...) {
        Logger::Log(LogLevel::kError, "write completion callback threw");
    }
}

}  // namespace

class EpollTcpServerBackend final : public TcpServerBackend {
public:
    using OnOpenFn = TcpServer::OnOpenFn;
    using OnFrameFn = TcpServer::OnFrameFn;
    using OnCloseFn = TcpServer::OnCloseFn;
    using OnBackpressureFn = TcpServer::OnBackpressureFn;

    EpollTcpServerBackend();
    ~EpollTcpServerBackend() override;

    EpollTcpServerBackend(const EpollTcpServerBackend&) = delete;
    EpollTcpServerBackend& operator=(const EpollTcpServerBackend&) = delete;

    void SetOnOpen(OnOpenFn fn) override;
    void SetOnFrame(OnFrameFn fn) override;
    void SetOnClose(OnCloseFn fn) override;
    void SetOnBackpressure(OnBackpressureFn fn) override;
    void SetOptions(const TcpServerOptions& opts) override;

    Status Start(const Endpoint& endpoint) override;
    void Stop() override;
    void StopAccepting() override;
    bool running() const override;

    Status SendFrame(ConnectionId conn_id,
                     uint64_t generation,
                     const ProtocolFrame& frame,
                     bool close_after_send,
                     std::function<void(Status)> completion) override;
    void CloseConnection(ConnectionId conn_id, uint64_t generation) override;

private:
    struct PendingWrite {
        std::string data;
        std::size_t offset = 0;
        std::function<void(Status)> completion;
    };

    struct Connection {
        ConnectionId id = 0;
        int fd = -1;
        uint64_t generation = 0;
        std::string read_buffer;
        std::deque<PendingWrite> write_buffers;
        std::size_t write_buffer_bytes = 0;
        bool closing = false;
        bool in_backpressure = false;
    };

    struct QueuedResponse {
        ConnectionId conn_id = 0;
        uint64_t generation = 0;
        std::string data;
        bool close_after_send = false;
        std::function<void(Status)> completion;
    };

    struct QueuedClose {
        ConnectionId conn_id = 0;
        uint64_t generation = 0;
    };

    Status SetupListener();
    void CleanupAfterJoin();
    void RunEventLoop();
    void AcceptConnections();
    void HandleClientRead(int client_fd);
    void DrainResponses();
    void DrainCloseRequests();
    void DrainWakeEvents();
    void WakeEventLoop();
    void CloseListenFd();
    void CloseAllConnections();
    Connection* FindByFd(int fd);
    Connection* FindById(ConnectionId conn_id);
    bool MatchesGeneration(ConnectionId conn_id, uint64_t generation) const;
    bool HasPendingWrites(const Connection& conn) const;
    void QueueWriteBuffer(Connection& conn, PendingWrite&& write);
    bool SendToConnection(Connection& conn, PendingWrite&& write);
    void FailPendingWrites(Connection& conn, const char* message);
    void FailQueuedResponses(const char* message);
    void FlushWriteBuffer(int client_fd);
    bool UpdateInterest(Connection& conn, bool want_write);
    bool SetBackpressure(Connection& conn, bool enabled);
    bool MaybeEnterBackpressure(Connection& conn);
    bool MaybeLeaveBackpressure(Connection& conn);
    void CloseByFd(int client_fd);
    void CloseById(ConnectionId conn_id);

    std::atomic<bool> running_;
    std::atomic<bool> accepting_;
    Endpoint endpoint_;
    TcpServerOptions options_;
    RpcCodec codec_;
    OnOpenFn on_open_;
    OnFrameFn on_frame_;
    OnCloseFn on_close_;
    OnBackpressureFn on_backpressure_;

    std::thread reactor_thread_;
    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    std::unordered_map<int, ConnectionId> connection_by_fd_;
    std::unordered_map<ConnectionId, Connection> connections_;
    ConnectionId next_connection_id_ = 1;
    uint64_t next_generation_ = 1;
    std::mutex response_mutex_;
    std::mutex wake_mutex_;
    std::vector<QueuedResponse> response_queue_;
    std::vector<QueuedClose> close_queue_;
};

EpollTcpServerBackend::EpollTcpServerBackend()
    : running_(false),
      accepting_(false) {}

EpollTcpServerBackend::~EpollTcpServerBackend() { Stop(); }

void EpollTcpServerBackend::SetOnOpen(OnOpenFn fn) { on_open_ = std::move(fn); }
void EpollTcpServerBackend::SetOnFrame(OnFrameFn fn) { on_frame_ = std::move(fn); }
void EpollTcpServerBackend::SetOnClose(OnCloseFn fn) { on_close_ = std::move(fn); }
void EpollTcpServerBackend::SetOnBackpressure(OnBackpressureFn fn) { on_backpressure_ = std::move(fn); }
void EpollTcpServerBackend::SetOptions(const TcpServerOptions& opts) { options_ = opts; }
Status EpollTcpServerBackend::Start(const Endpoint& endpoint) {
    if (running_.load(std::memory_order_acquire)) {
        return Status::Ok();
    }
    if (reactor_thread_.joinable()) {
        if (current_epoll_reactor == this) {
            return Status::Error(StatusCode::kNetworkError, "cannot restart epoll reactor from its callback");
        }
        reactor_thread_.join();
        CleanupAfterJoin();
    }
    endpoint_ = endpoint;

    Status status = SetupListener();
    if (!status.ok()) {
        Stop();
        return status;
    }
    accepting_.store(true, std::memory_order_release);
    running_.store(true, std::memory_order_release);
    try {
        reactor_thread_ = std::thread([this] {
            current_epoll_reactor = this;
            RunEventLoop();
            current_epoll_reactor = nullptr;
        });
    } catch (const std::system_error& error) {
        running_.store(false, std::memory_order_release);
        accepting_.store(false, std::memory_order_release);
        CleanupAfterJoin();
        return Status::Error(StatusCode::kNetworkError,
                             std::string("failed to start epoll reactor thread: ") + error.what());
    }
    return Status::Ok();
}

void EpollTcpServerBackend::Stop() {
    running_.store(false, std::memory_order_release);
    accepting_.store(false, std::memory_order_release);

    WakeEventLoop();
    if (reactor_thread_.joinable()) {
        if (current_epoll_reactor == this) {
            return;
        }
        reactor_thread_.join();
    }
    CleanupAfterJoin();
}

void EpollTcpServerBackend::CleanupAfterJoin() {
    CloseListenFd();
    CloseAllConnections();
    FailQueuedResponses("TcpServer stopped before response was sent");
    CloseFd(&epoll_fd_);
    {
        std::lock_guard<std::mutex> lock(wake_mutex_);
        CloseFd(&wake_fd_);
    }
}

bool EpollTcpServerBackend::running() const {
    return running_.load(std::memory_order_acquire);
}

void EpollTcpServerBackend::StopAccepting() {
    accepting_.store(false, std::memory_order_release);
    WakeEventLoop();
}

Status EpollTcpServerBackend::SendFrame(ConnectionId conn_id,
                                        uint64_t generation,
                                        const ProtocolFrame& frame,
                                        bool close_after_send,
                                        std::function<void(Status)> completion) {
    if (!running_.load(std::memory_order_acquire)) {
        return Status::Error(StatusCode::kNetworkError, "TcpServer is stopped");
    }
    std::string data = codec_.Encode(frame);
    bool overflow = false;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        if (!running_.load(std::memory_order_acquire)) {
            return Status::Error(StatusCode::kNetworkError, "TcpServer is stopped");
        }
        if (response_queue_.size() >= options_.max_response_queue) {
            close_queue_.push_back({conn_id, generation});
            overflow = true;
        } else {
            response_queue_.push_back(
                {conn_id, generation, std::move(data), close_after_send, std::move(completion)});
        }
    }
    WakeEventLoop();
    if (overflow) {
        return Status::Error(StatusCode::kServerError, "response queue full");
    }
    return Status::Ok();
}

void EpollTcpServerBackend::CloseConnection(ConnectionId conn_id, uint64_t generation) {
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        if (!running_.load(std::memory_order_acquire)) {
            return;
        }
        close_queue_.push_back({conn_id, generation});
    }
    WakeEventLoop();
}

Status EpollTcpServerBackend::SetupListener() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("socket failed", errno));
    }
    const int enabled = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) == -1) {
        return Status::Error(StatusCode::kNetworkError,
                             LastSocketError("setsockopt(SO_REUSEADDR) failed", errno));
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(endpoint_.port);
    if (::inet_pton(AF_INET, endpoint_.host.c_str(), &address.sin_addr) != 1) {
        return Status::Error(StatusCode::kNetworkError, "invalid listen host: " + endpoint_.host);
    }
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("bind failed", errno));
    }
    if (::listen(listen_fd_, options_.backlog) == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("listen failed", errno));
    }
    std::string error;
    if (!SetNonBlocking(listen_fd_, &error)) {
        return Status::Error(StatusCode::kNetworkError, error);
    }
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("epoll_create1 failed", errno));
    }
    const int wake_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("eventfd failed", errno));
    }
    {
        std::lock_guard<std::mutex> lock(wake_mutex_);
        CloseFd(&wake_fd_);
        wake_fd_ = wake_fd;
    }
    epoll_event wake_event{};
    wake_event.events = EPOLLIN | EPOLLET;
    wake_event.data.fd = wake_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wake_event) == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("epoll_ctl wake fd failed", errno));
    }
    epoll_event listen_event{};
    listen_event.events = EPOLLIN | EPOLLET;
    listen_event.data.fd = listen_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &listen_event) == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("epoll_ctl listen fd failed", errno));
    }
    return Status::Ok();
}

void EpollTcpServerBackend::RunEventLoop() {
    epoll_event events[kMaxEvents];
    while (running_.load(std::memory_order_acquire)) {
        if (!accepting_.load(std::memory_order_acquire)) {
            CloseListenFd();
        }
        const int ready = ::epoll_wait(epoll_fd_, events, kMaxEvents, 1000);
        if (ready == -1) {
            if (errno == EINTR) continue;
            running_.store(false, std::memory_order_release);
            break;
        }
        for (int i = 0; i < ready; ++i) {
            const int fd = events[i].data.fd;
            if (fd == listen_fd_) {
                if (accepting_.load(std::memory_order_acquire)) {
                    AcceptConnections();
                }
                continue;
            }
            if (fd == wake_fd_)   { DrainWakeEvents(); continue; }
            if ((events[i].events & EPOLLIN)  != 0) HandleClientRead(fd);
            if ((events[i].events & EPOLLOUT) != 0) FlushWriteBuffer(fd);
            if ((events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
                CloseByFd(fd);
            }
        }
        DrainResponses();
    }
    DrainResponses();
    accepting_.store(false, std::memory_order_release);
    CleanupAfterJoin();
}

void EpollTcpServerBackend::AcceptConnections() {
    if (listen_fd_ == -1 || !accepting_.load(std::memory_order_acquire)) {
        return;
    }
    while (true) {
        const int client_fd = ::accept4(
            listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd == -1) return;
        if (options_.max_connections != 0 &&
            connections_.size() >= options_.max_connections) {
            ::close(client_fd);
            continue;
        }

        epoll_event event{};
        event.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
        event.data.fd = client_fd;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &event) == -1) {
            ::close(client_fd);
            continue;
        }
        const ConnectionId id = next_connection_id_++;
        const uint64_t gen = next_generation_++;
        Connection conn;
        conn.id = id;
        conn.fd = client_fd;
        conn.generation = gen;
        connection_by_fd_[client_fd] = id;
        connections_[id] = std::move(conn);
        if (on_open_) on_open_(id, gen);
    }
}

void EpollTcpServerBackend::CloseListenFd() {
    if (listen_fd_ != -1) {
        if (epoll_fd_ != -1) {
            (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, listen_fd_, nullptr);
        }
        CloseFd(&listen_fd_);
    }
}

void EpollTcpServerBackend::CloseAllConnections() {
    std::vector<ConnectionId> ids;
    ids.reserve(connections_.size());
    for (const auto& entry : connections_) {
        ids.push_back(entry.first);
    }
    for (ConnectionId id : ids) {
        CloseById(id);
    }
}

void EpollTcpServerBackend::HandleClientRead(int client_fd) {
    Connection* conn = FindByFd(client_fd);
    if (conn == nullptr || conn->closing || conn->in_backpressure) return;

    char temp[kReadChunkSize];
    while (true) {
        const ssize_t n = ::recv(client_fd, temp, sizeof(temp), 0);
        if (n > 0) {
            conn->read_buffer.append(temp, static_cast<std::size_t>(n));

            while (true) {
                ProtocolFrame frame;
                std::string error;
                const DecodeResult result = codec_.TryDecode(conn->read_buffer, &frame, &error);
                if (result == DecodeResult::kNeedMoreData) {
                    if (conn->read_buffer.size() > options_.max_read_buffer_bytes) {
                        CloseByFd(client_fd);
                        return;
                    }
                    break;
                }
                if (result == DecodeResult::kProtocolError) {
                    CloseByFd(client_fd);
                    return;
                }

                const ConnectionId id = conn->id;
                const uint64_t generation = conn->generation;
                if (on_frame_) {
                    on_frame_(id, generation, std::move(frame));
                }
                conn = FindByFd(client_fd);
                if (conn == nullptr) {
                    return;
                }
            }
            continue;
        }
        if (n == 0)                                  { CloseByFd(client_fd); return; }
        if (errno == EINTR)                          continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        CloseByFd(client_fd);
        return;
    }
}

void EpollTcpServerBackend::DrainResponses() {
    DrainCloseRequests();

    std::vector<QueuedResponse> pending;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        pending.swap(response_queue_);
    }
    for (auto& r : pending) {
        Connection* conn = FindById(r.conn_id);
        if (conn == nullptr || (r.generation != 0 && conn->generation != r.generation)) {
            CompleteWrite(
                &r.completion,
                Status::Error(StatusCode::kNetworkError, "connection closed before response was sent"));
            continue;
        }
        PendingWrite write{std::move(r.data), 0, std::move(r.completion)};
        if (!SendToConnection(*conn, std::move(write))) {
            CloseById(r.conn_id);
            continue;
        }
        if (r.close_after_send) {
            if (!HasPendingWrites(*conn)) CloseById(r.conn_id);
            else                          conn->closing = true;
        }
    }
    DrainCloseRequests();
}

void EpollTcpServerBackend::DrainCloseRequests() {
    std::vector<QueuedClose> pending;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        pending.swap(close_queue_);
    }
    for (const auto& c : pending) {
        if (MatchesGeneration(c.conn_id, c.generation)) CloseById(c.conn_id);
    }
}

void EpollTcpServerBackend::DrainWakeEvents() {
    uint64_t value = 0;
    while (true) {
        const ssize_t n = ::read(wake_fd_, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) continue;
        if (n == -1 && errno == EINTR) continue;
        return;
    }
}

void EpollTcpServerBackend::WakeEventLoop() {
    std::lock_guard<std::mutex> lock(wake_mutex_);
    if (wake_fd_ == -1) return;
    const uint64_t value = 1;
    while (true) {
        const ssize_t n = ::write(wake_fd_, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) return;
        if (n == -1 && errno == EINTR) continue;
        return;
    }
}

EpollTcpServerBackend::Connection* EpollTcpServerBackend::FindByFd(int fd) {
    const auto it = connection_by_fd_.find(fd);
    if (it == connection_by_fd_.end()) return nullptr;
    return FindById(it->second);
}

EpollTcpServerBackend::Connection* EpollTcpServerBackend::FindById(ConnectionId id) {
    const auto it = connections_.find(id);
    if (it == connections_.end()) return nullptr;
    return &it->second;
}

bool EpollTcpServerBackend::MatchesGeneration(ConnectionId id, uint64_t generation) const {
    const auto it = connections_.find(id);
    if (it == connections_.end()) return false;
    return generation == 0 || it->second.generation == generation;
}

bool EpollTcpServerBackend::HasPendingWrites(const Connection& conn) const {
    return conn.write_buffer_bytes > 0;
}

void EpollTcpServerBackend::QueueWriteBuffer(Connection& conn, PendingWrite&& write) {
    if (write.offset >= write.data.size()) {
        CompleteWrite(&write.completion, Status::Ok());
        return;
    }
    conn.write_buffer_bytes += write.data.size() - write.offset;
    conn.write_buffers.push_back(std::move(write));
}

bool EpollTcpServerBackend::SendToConnection(Connection& conn, PendingWrite&& write) {
    if (write.data.empty()) {
        CompleteWrite(&write.completion, Status::Ok());
        return true;
    }
    if (HasPendingWrites(conn)) {
        QueueWriteBuffer(conn, std::move(write));
        if (!UpdateInterest(conn, true)) return false;
        if (!MaybeEnterBackpressure(conn)) return false;
        return conn.write_buffer_bytes <= options_.max_write_buffer_bytes;
    }
    const SendResult sent = SendAll(conn.fd, write.data.data(), write.data.size());
    if (sent.status == SendStatus::kOk) {
        CompleteWrite(&write.completion, Status::Ok());
        return true;
    }
    if (sent.status == SendStatus::kIoError) {
        CompleteWrite(
            &write.completion,
            Status::Error(StatusCode::kNetworkError, "send failed before response was completed"));
        return false;
    }
    write.offset = sent.sent;
    QueueWriteBuffer(conn, std::move(write));
    if (conn.write_buffer_bytes > options_.max_write_buffer_bytes) return false;
    if (!UpdateInterest(conn, true)) return false;
    if (!MaybeEnterBackpressure(conn)) return false;
    return true;
}

void EpollTcpServerBackend::FlushWriteBuffer(int client_fd) {
    Connection* conn = FindByFd(client_fd);
    if (conn == nullptr || !HasPendingWrites(*conn)) return;

    while (HasPendingWrites(*conn)) {
        PendingWrite& front = conn->write_buffers.front();
        const std::size_t remaining = front.data.size() - front.offset;
        const SendResult sent =
            SendAll(client_fd, front.data.data() + front.offset, remaining);
        conn = FindByFd(client_fd);
        if (conn == nullptr) return;
        if (sent.status == SendStatus::kIoError) {
            CloseById(conn->id);
            return;
        }

        conn->write_buffer_bytes -= sent.sent;
        if (sent.status == SendStatus::kWouldBlock) {
            front.offset += sent.sent;
            if (!MaybeLeaveBackpressure(*conn)) return;
            return;
        }

        std::function<void(Status)> completion = std::move(front.completion);
        conn->write_buffers.pop_front();
        CompleteWrite(&completion, Status::Ok());
        conn = FindByFd(client_fd);
        if (conn == nullptr) return;
    }

    if (!MaybeLeaveBackpressure(*conn)) return;
    if (!UpdateInterest(*conn, false)) {
        CloseById(conn->id);
        return;
    }
    if (conn->closing) CloseById(conn->id);
}

bool EpollTcpServerBackend::UpdateInterest(Connection& conn, bool want_write) {
    epoll_event event{};
    event.data.fd = conn.fd;
    event.events = EPOLLRDHUP | EPOLLET;
    if (!conn.in_backpressure) event.events |= EPOLLIN;
    if (want_write || HasPendingWrites(conn)) event.events |= EPOLLOUT;
    return ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn.fd, &event) != -1;
}

bool EpollTcpServerBackend::SetBackpressure(Connection& conn, bool enabled) {
    if (conn.in_backpressure == enabled) {
        return true;
    }
    conn.in_backpressure = enabled;

    std::ostringstream message;
    message << (enabled ? "enter backpressure" : "leave backpressure")
            << " fd=" << conn.fd
            << " write_buffer_size=" << conn.write_buffer_bytes;
    Logger::Log(LogLevel::kWarn, message.str());

    if (on_backpressure_) {
        on_backpressure_(conn.id, conn.generation, conn.fd, enabled, conn.write_buffer_bytes);
    }
    return UpdateInterest(conn, HasPendingWrites(conn));
}

bool EpollTcpServerBackend::MaybeEnterBackpressure(Connection& conn) {
    if (!conn.in_backpressure && conn.write_buffer_bytes >= options_.high_watermark_bytes) {
        if (!SetBackpressure(conn, true)) {
            CloseById(conn.id);
            return false;
        }
    }
    return true;
}

bool EpollTcpServerBackend::MaybeLeaveBackpressure(Connection& conn) {
    if (conn.in_backpressure && conn.write_buffer_bytes <= options_.low_watermark_bytes) {
        if (!SetBackpressure(conn, false)) {
            CloseById(conn.id);
            return false;
        }
    }
    return true;
}

void EpollTcpServerBackend::CloseByFd(int client_fd) {
    const auto it = connection_by_fd_.find(client_fd);
    if (it == connection_by_fd_.end()) {
        return;
    }
    CloseById(it->second);
}

void EpollTcpServerBackend::CloseById(ConnectionId conn_id) {
    auto it = connections_.find(conn_id);
    if (it == connections_.end()) return;
    const int client_fd = it->second.fd;
    const uint64_t gen = it->second.generation;
    FailPendingWrites(it->second, "connection closed before response was sent");
    if (it->second.in_backpressure) {
        std::ostringstream message;
        message << "leave backpressure"
                << " fd=" << client_fd
                << " write_buffer_size=" << it->second.write_buffer_bytes;
        Logger::Log(LogLevel::kWarn, message.str());
        if (on_backpressure_) {
            on_backpressure_(conn_id, gen, client_fd, false, it->second.write_buffer_bytes);
        }
    }
    connection_by_fd_.erase(client_fd);
    connections_.erase(it);
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
    ::close(client_fd);
    if (on_close_) on_close_(conn_id, gen);
}

void EpollTcpServerBackend::FailPendingWrites(Connection& conn, const char* message) {
    while (!conn.write_buffers.empty()) {
        PendingWrite write = std::move(conn.write_buffers.front());
        conn.write_buffers.pop_front();
        CompleteWrite(
            &write.completion,
            Status::Error(StatusCode::kNetworkError, message));
    }
    conn.write_buffer_bytes = 0;
}

void EpollTcpServerBackend::FailQueuedResponses(const char* message) {
    std::vector<QueuedResponse> pending;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        pending.swap(response_queue_);
        close_queue_.clear();
    }
    for (QueuedResponse& response : pending) {
        CompleteWrite(
            &response.completion,
            Status::Error(StatusCode::kNetworkError, message));
    }
}

std::unique_ptr<TcpServerBackend> MakeEpollTcpServerBackend() {
    return std::make_unique<EpollTcpServerBackend>();
}

}  // namespace minirpc
