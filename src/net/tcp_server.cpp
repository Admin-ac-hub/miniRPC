#include "minirpc/net/tcp_server.h"

#include <cerrno>
#include <cstring>
#include <utility>

#include "minirpc/net/socket_utils.h"

#include <arpa/inet.h>
#include <fcntl.h>
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
      listen_fd_(-1),
      epoll_fd_(-1),
      wake_fd_(-1),
      next_connection_id_(1),
      next_generation_(1)
{}

TcpServer::~TcpServer() { Stop(); }

void TcpServer::SetOnFrame(OnFrameFn fn) { on_frame_ = std::move(fn); }
void TcpServer::SetOnClose(OnCloseFn fn) { on_close_ = std::move(fn); }
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
    running_.store(true, std::memory_order_release);
    reactor_thread_ = std::thread([this] { RunEventLoop(); });
    return Status::Ok();
}

void TcpServer::Stop() {
    running_.store(false, std::memory_order_release);

    if (wake_fd_ != -1) {
        const uint64_t value = 1;
        (void)::write(wake_fd_, &value, sizeof(value));
    }
    if (reactor_thread_.joinable()) {
        reactor_thread_.join();
    }
    CloseFd(&listen_fd_);

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

Status TcpServer::SendFrame(ConnectionId conn_id,
                            uint64_t generation,
                            const ProtocolFrame& frame,
                            bool close_after_send) {
    if (!running_.load(std::memory_order_acquire)) {
        return Status::Error(StatusCode::kNetworkError, "TcpServer is stopped");
    }
    const std::string data = codec_.Encode(frame);
    bool overflow = false;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        if (response_queue_.size() >= options_.max_response_queue) {
            close_queue_.push_back({conn_id, generation});
            overflow = true;
        } else {
            response_queue_.push_back({conn_id, generation, data, close_after_send});
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
        const int ready = ::epoll_wait(epoll_fd_, events, kMaxEvents, 1000);
        if (ready == -1) {
            if (errno == EINTR) continue;
            break;
        }
        for (int i = 0; i < ready; ++i) {
            const int fd = events[i].data.fd;
            if (fd == listen_fd_) { AcceptConnections(); continue; }
            if (fd == wake_fd_)   { DrainWakeEvents(); DrainResponses(); continue; }
            if ((events[i].events & EPOLLIN)  != 0) HandleClientRead(fd);
            if ((events[i].events & EPOLLOUT) != 0) FlushWriteBuffer(fd);
            if ((events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
                CloseByFd(fd);
            }
        }
        DrainResponses();
    }
}

void TcpServer::AcceptConnections() {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        const int client_fd = ::accept4(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr),
                                        &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
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
    }
}

void TcpServer::HandleClientRead(int client_fd) {
    Connection* conn = FindByFd(client_fd);
    if (conn == nullptr || conn->closing) return;

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
    for (const auto& r : pending) {
        Connection* conn = FindById(r.conn_id);
        if (conn == nullptr || !MatchesGeneration(r.conn_id, r.generation)) continue;
        if (!SendToConnection(*conn, r.data)) {
            CloseById(r.conn_id);
            continue;
        }
        if (r.close_after_send) {
            if (conn->write_buffer.empty()) CloseById(r.conn_id);
            else                            conn->closing = true;
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

bool TcpServer::SendToConnection(Connection& conn, const std::string& data) {
    if (!conn.write_buffer.empty()) {
        conn.write_buffer += data;
        return conn.write_buffer.size() <= options_.max_write_buffer_bytes;
    }
    const SendResult sent = SendAll(conn.fd, data.data(), data.size());
    if (sent.status == SendStatus::kOk)      return true;
    if (sent.status == SendStatus::kIoError) return false;
    conn.write_buffer = data.substr(sent.sent);
    if (conn.write_buffer.size() > options_.max_write_buffer_bytes) return false;
    return SetWriteMode(conn.fd, true);
}

void TcpServer::FlushWriteBuffer(int client_fd) {
    Connection* conn = FindByFd(client_fd);
    if (conn == nullptr || conn->write_buffer.empty()) return;
    const SendResult sent = SendAll(client_fd, conn->write_buffer.data(), conn->write_buffer.size());
    if (sent.status == SendStatus::kWouldBlock) {
        conn->write_buffer.erase(0, sent.sent);
        return;
    }
    if (sent.status == SendStatus::kIoError) {
        CloseById(conn->id);
        return;
    }
    conn->write_buffer.clear();
    if (!SetWriteMode(client_fd, false)) {
        CloseById(conn->id);
        return;
    }
    if (conn->closing) CloseById(conn->id);
}

bool TcpServer::SetWriteMode(int client_fd, bool enable) {
    epoll_event event{};
    event.data.fd = client_fd;
    event.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
    if (enable) event.events |= EPOLLOUT;
    return ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &event) != -1;
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
    connection_by_fd_.erase(client_fd);
    connections_.erase(it);
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
    ::close(client_fd);
    if (on_close_) on_close_(conn_id, gen);
}

}  // namespace minirpc
