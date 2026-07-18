#include "minirpc/server/coroutine_rpc_server.h"

#include <cerrno>
#include <chrono>
#include <thread>
#include <utility>
#include <vector>

#include "minirpc/net/socket_utils.h"
#include "minirpc/server/coroutine_rpc_connection.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

namespace minirpc {
namespace {

constexpr auto kDefaultStopGrace = std::chrono::milliseconds(1000);

}  // namespace

CoroutineRpcServer::CoroutineRpcServer(std::size_t worker_count, std::size_t max_queue_size)
    : running_(false),
      thread_pool_(worker_count, max_queue_size),
      listen_fd_(-1) {}

CoroutineRpcServer::~CoroutineRpcServer() {
    Stop();
}

void CoroutineRpcServer::RegisterService(const std::string& service_name,
                                         const std::string& method_name,
                                         RpcHandler handler) {
    registry_.Register(service_name, method_name, std::move(handler));
}

Status CoroutineRpcServer::Start(const Endpoint& endpoint) {
    if (running_.load(std::memory_order_acquire)) {
        return Status::Ok();
    }
    endpoint_ = endpoint;
    Status status = SetupListener();
    if (!status.ok()) {
        CloseListenFd();
        return status;
    }

    thread_pool_.Start();
    running_.store(true, std::memory_order_release);
    io_.Spawn([this] { AcceptLoop(); });
    io_thread_ = std::thread([this] { io_.Run(); });
    return Status::Ok();
}

void CoroutineRpcServer::Stop() {
    Stop(kDefaultStopGrace);
}

void CoroutineRpcServer::Stop(std::chrono::milliseconds grace_period) {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    CloseListenFd();
    WaitForPendingRequests(grace_period);
    CloseAllClientFds();
    io_.Stop();
    if (io_thread_.joinable()) {
        io_thread_.join();
    }
    thread_pool_.Stop();
}

bool CoroutineRpcServer::running() const {
    return running_.load(std::memory_order_acquire);
}

const RpcMetrics& CoroutineRpcServer::metrics() const {
    return metrics_;
}

std::size_t CoroutineRpcServer::threadpool_queue_size() const {
    return thread_pool_.queued_tasks();
}

std::string CoroutineRpcServer::MetricsText() const {
    return metrics_.ToPrometheusText(threadpool_queue_size());
}

Status CoroutineRpcServer::SetupListener() {
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
    if (::listen(listen_fd_, SOMAXCONN) == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("listen failed", errno));
    }

    std::string error;
    if (!SetNonBlocking(listen_fd_, &error)) {
        return Status::Error(StatusCode::kNetworkError, error);
    }
    return Status::Ok();
}

void CoroutineRpcServer::AcceptLoop() {
    while (running_.load(std::memory_order_acquire)) {
        if (!io_.WaitReadable(listen_fd_)) {
            return;
        }

        while (running_.load(std::memory_order_acquire)) {
            const int client_fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (client_fd == -1) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                return;
            }

            TrackClientFd(client_fd);
            io_.Spawn([this, client_fd] {
                CoroutineRpcConnection connection(&io_, &registry_, &thread_pool_, &metrics_);
                (void)connection.Serve(client_fd);
                CloseClientFd(client_fd);
            });
        }
    }
}

void CoroutineRpcServer::TrackClientFd(int fd) {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    if (client_fds_.insert(fd).second) {
        metrics_.IncrementActiveConnections();
    }
}

void CoroutineRpcServer::CloseClientFd(int fd) {
    bool should_close = false;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        should_close = client_fds_.erase(fd) > 0;
    }
    if (should_close) {
        ::close(fd);
        metrics_.DecrementActiveConnections();
    }
}

void CoroutineRpcServer::CloseAllClientFds() {
    std::vector<int> fds;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        fds.assign(client_fds_.begin(), client_fds_.end());
        client_fds_.clear();
    }
    for (int fd : fds) {
        ::close(fd);
        metrics_.DecrementActiveConnections();
    }
}

void CoroutineRpcServer::CloseListenFd() {
    if (listen_fd_ != -1) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

void CoroutineRpcServer::WaitForPendingRequests(std::chrono::milliseconds grace_period) const {
    if (grace_period <= std::chrono::milliseconds::zero()) {
        return;
    }
    const auto deadline = std::chrono::steady_clock::now() + grace_period;
    while (metrics_.pending_requests() > 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

}  // namespace minirpc
