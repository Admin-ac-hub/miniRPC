#include "minirpc/net/tcp_server.h"

#include <cerrno>
#include <sstream>
#include <utility>

#include "minirpc/net/socket_utils.h"
#include "minirpc/observability/logger.h"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace minirpc {
namespace {

constexpr int kMaxEvents = 64;
constexpr std::size_t kReadChunkSize = 4096;

void CloseFd(int* fd) {
    if (*fd != -1) {
        ::close(*fd);
        *fd = -1;
    }
}

}  // namespace

TcpServer::TcpServer()
    : running_(false),
      accepting_(false) {}

TcpServer::~TcpServer() { Stop(); }

void TcpServer::SetOnOpen(OnOpenFn fn) { on_open_ = std::move(fn); }
void TcpServer::SetOnFrame(OnFrameFn fn) { on_frame_ = std::move(fn); }
void TcpServer::SetOnClose(OnCloseFn fn) { on_close_ = std::move(fn); }
void TcpServer::SetOnBackpressure(OnBackpressureFn fn) { on_backpressure_ = std::move(fn); }
void TcpServer::SetOptions(const TcpServerOptions& opts) { options_ = opts; }
Status TcpServer::Start(const Endpoint& endpoint) {
    if (running_.load(std::memory_order_acquire)) {
        return Status::Ok();
    }
    endpoint_ = endpoint;

    Status status = SetupListener();
    if (!status.ok()) {
        Stop();
        return status;
    }
    accepting_.store(true, std::memory_order_release);
    running_.store(true, std::memory_order_release);
    reactor_thread_ = std::thread([this] { RunEventLoop(); });
    return Status::Ok();
}

void TcpServer::Stop() {
    running_.store(false, std::memory_order_release);
    accepting_.store(false, std::memory_order_release);

    WakeEventLoop();
    if (reactor_thread_.joinable()) {
        reactor_thread_.join();
    }
    CloseListenFd();

    std::vector<ConnectionId> ids;
    ids.reserve(connections_.size());
    for (const auto& [id, _] : connections_) {
        ids.push_back(id);
    }
    for (ConnectionId id : ids) {
        CloseById(id);
    }
    CloseFd(&epoll_fd_);
    CloseFd(&wake_fd_);
}

bool TcpServer::running() const {
    return running_.load(std::memory_order_acquire);
}

void TcpServer::StopAccepting() {
    accepting_.store(false, std::memory_order_release);
    WakeEventLoop();
}

Status TcpServer::SendFrame(ConnectionId conn_id,
                            uint64_t generation,
                            const ProtocolFrame& frame,
                            bool close_after_send) {
    if (!running_.load(std::memory_order_acquire)) {
        return Status::Error(StatusCode::kNetworkError, "TcpServer is stopped");
    }
    std::string data = codec_.Encode(frame);
    bool overflow = false;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        if (response_queue_.size() >= options_.max_response_queue) {
            close_queue_.push_back({conn_id, generation});
            overflow = true;
        } else {
            response_queue_.push_back({conn_id, generation, std::move(data), close_after_send});
        }
    }
    WakeEventLoop();
    if (overflow) {
        return Status::Error(StatusCode::kServerError, "response queue full");
    }
    return Status::Ok();
}

void TcpServer::CloseConnection(ConnectionId conn_id, uint64_t generation) {
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        close_queue_.push_back({conn_id, generation});
    }
    WakeEventLoop();
}

Status TcpServer::SetupListener() {
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
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("eventfd failed", errno));
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

void TcpServer::RunEventLoop() {
    epoll_event events[kMaxEvents];
    while (running_.load(std::memory_order_acquire)) {
        if (!accepting_.load(std::memory_order_acquire)) {
            CloseListenFd();
        }
        const int ready = ::epoll_wait(epoll_fd_, events, kMaxEvents, 1000);
        if (ready == -1) {
            if (errno == EINTR) continue;
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
}

void TcpServer::AcceptConnections() {
    if (listen_fd_ == -1 || !accepting_.load(std::memory_order_acquire)) {
        return;
    }
    while (true) {
        const int client_fd = ::accept4(
            listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd == -1) return;

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

void TcpServer::CloseListenFd() {
    if (listen_fd_ != -1) {
        if (epoll_fd_ != -1) {
            (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, listen_fd_, nullptr);
        }
        CloseFd(&listen_fd_);
    }
}

void TcpServer::HandleClientRead(int client_fd) {
    Connection* conn = FindByFd(client_fd);
    if (conn == nullptr || conn->closing || conn->in_backpressure) return;

    char temp[kReadChunkSize];
    while (true) {
        const ssize_t n = ::recv(client_fd, temp, sizeof(temp), 0);
        if (n > 0) {
            conn->read_buffer.append(temp, static_cast<std::size_t>(n));
            if (conn->read_buffer.size() > options_.max_read_buffer_bytes) {
                CloseByFd(client_fd);
                return;
            }
            continue;
        }
        if (n == 0)                                  { CloseByFd(client_fd); return; }
        if (errno == EINTR)                          continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        CloseByFd(client_fd);
        return;
    }

    while (true) {
        ProtocolFrame frame;
        std::string error;
        const DecodeResult r = codec_.TryDecode(conn->read_buffer, &frame, &error);
        if (r == DecodeResult::kNeedMoreData) return;
        if (r == DecodeResult::kProtocolError) { CloseByFd(client_fd); return; }
        const ConnectionId id = conn->id;
        const uint64_t gen = conn->generation;
        if (on_frame_) on_frame_(id, gen, std::move(frame));
        conn = FindByFd(client_fd);
        if (conn == nullptr) return;
    }
}

void TcpServer::DrainResponses() {
    DrainCloseRequests();

    std::vector<QueuedResponse> pending;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        pending.swap(response_queue_);
    }
    for (auto& r : pending) {
        Connection* conn = FindById(r.conn_id);
        if (conn == nullptr || (r.generation != 0 && conn->generation != r.generation)) continue;
        if (!SendToConnection(*conn, std::move(r.data))) {
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

void TcpServer::DrainCloseRequests() {
    std::vector<QueuedClose> pending;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        pending.swap(close_queue_);
    }
    for (const auto& c : pending) {
        if (MatchesGeneration(c.conn_id, c.generation)) CloseById(c.conn_id);
    }
}

void TcpServer::DrainWakeEvents() {
    uint64_t value = 0;
    while (true) {
        const ssize_t n = ::read(wake_fd_, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) continue;
        if (n == -1 && errno == EINTR) continue;
        return;
    }
}

void TcpServer::WakeEventLoop() {
    if (wake_fd_ == -1) return;
    const uint64_t value = 1;
    while (true) {
        const ssize_t n = ::write(wake_fd_, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) return;
        if (n == -1 && errno == EINTR) continue;
        return;
    }
}

TcpServer::Connection* TcpServer::FindByFd(int fd) {
    const auto it = connection_by_fd_.find(fd);
    if (it == connection_by_fd_.end()) return nullptr;
    return FindById(it->second);
}

TcpServer::Connection* TcpServer::FindById(ConnectionId id) {
    const auto it = connections_.find(id);
    if (it == connections_.end()) return nullptr;
    return &it->second;
}

bool TcpServer::MatchesGeneration(ConnectionId id, uint64_t generation) const {
    const auto it = connections_.find(id);
    if (it == connections_.end()) return false;
    return generation == 0 || it->second.generation == generation;
}

bool TcpServer::HasPendingWrites(const Connection& conn) const {
    return conn.write_buffer_bytes > 0;
}

void TcpServer::QueueWriteBuffer(Connection& conn, std::string&& data, std::size_t offset) {
    if (offset >= data.size()) {
        return;
    }
    const bool was_empty = conn.write_buffers.empty();
    conn.write_buffer_bytes += data.size() - offset;
    conn.write_buffers.push_back(std::move(data));
    if (was_empty) {
        conn.write_buffer_offset = offset;
    }
}

bool TcpServer::SendToConnection(Connection& conn, std::string&& data) {
    if (data.empty()) {
        return true;
    }
    if (HasPendingWrites(conn)) {
        QueueWriteBuffer(conn, std::move(data), 0);
        if (!UpdateInterest(conn, true)) return false;
        if (!MaybeEnterBackpressure(conn)) return false;
        return conn.write_buffer_bytes <= options_.max_write_buffer_bytes;
    }
    const SendResult sent = SendAll(conn.fd, data.data(), data.size());
    if (sent.status == SendStatus::kOk)      return true;
    if (sent.status == SendStatus::kIoError) return false;
    QueueWriteBuffer(conn, std::move(data), sent.sent);
    if (conn.write_buffer_bytes > options_.max_write_buffer_bytes) return false;
    if (!UpdateInterest(conn, true)) return false;
    if (!MaybeEnterBackpressure(conn)) return false;
    return true;
}

void TcpServer::FlushWriteBuffer(int client_fd) {
    Connection* conn = FindByFd(client_fd);
    if (conn == nullptr || !HasPendingWrites(*conn)) return;

    while (HasPendingWrites(*conn)) {
        std::string& front = conn->write_buffers.front();
        const std::size_t offset = conn->write_buffer_offset;
        const std::size_t remaining = front.size() - offset;
        const SendResult sent = SendAll(client_fd, front.data() + offset, remaining);
        conn = FindByFd(client_fd);
        if (conn == nullptr) return;
        if (sent.status == SendStatus::kIoError) {
            CloseById(conn->id);
            return;
        }

        conn->write_buffer_bytes -= sent.sent;
        if (sent.status == SendStatus::kWouldBlock) {
            conn->write_buffer_offset += sent.sent;
            if (!MaybeLeaveBackpressure(*conn)) return;
            return;
        }

        conn->write_buffers.pop_front();
        conn->write_buffer_offset = 0;
    }

    if (!MaybeLeaveBackpressure(*conn)) return;
    if (!UpdateInterest(*conn, false)) {
        CloseById(conn->id);
        return;
    }
    if (conn->closing) CloseById(conn->id);
}

bool TcpServer::UpdateInterest(Connection& conn, bool want_write) {
    epoll_event event{};
    event.data.fd = conn.fd;
    event.events = EPOLLRDHUP | EPOLLET;
    if (!conn.in_backpressure) event.events |= EPOLLIN;
    if (want_write || HasPendingWrites(conn)) event.events |= EPOLLOUT;
    return ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn.fd, &event) != -1;
}

bool TcpServer::SetBackpressure(Connection& conn, bool enabled) {
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

bool TcpServer::MaybeEnterBackpressure(Connection& conn) {
    if (!conn.in_backpressure && conn.write_buffer_bytes >= options_.high_watermark_bytes) {
        if (!SetBackpressure(conn, true)) {
            CloseById(conn.id);
            return false;
        }
    }
    return true;
}

bool TcpServer::MaybeLeaveBackpressure(Connection& conn) {
    if (conn.in_backpressure && conn.write_buffer_bytes <= options_.low_watermark_bytes) {
        if (!SetBackpressure(conn, false)) {
            CloseById(conn.id);
            return false;
        }
    }
    return true;
}

void TcpServer::CloseByFd(int client_fd) {
    const auto it = connection_by_fd_.find(client_fd);
    if (it == connection_by_fd_.end()) {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
        ::close(client_fd);
        return;
    }
    CloseById(it->second);
}

void TcpServer::CloseById(ConnectionId conn_id) {
    auto it = connections_.find(conn_id);
    if (it == connections_.end()) return;
    const int client_fd = it->second.fd;
    const uint64_t gen = it->second.generation;
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

}  // namespace minirpc
