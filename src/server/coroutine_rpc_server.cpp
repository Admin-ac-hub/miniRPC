#include "minirpc/server/coroutine_rpc_server.h"

#include <cerrno>
#include <chrono>
#include <future>
#include <memory>
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

uint64_t UnixTimeMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

}  // namespace

CoroutineRpcServer::CoroutineRpcServer(std::size_t worker_count, std::size_t max_queue_size)
    : running_(false),
      io_running_(false),
      state_(State::kStopped),
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
    std::lock_guard<std::mutex> operation_lock(operation_mutex_);
    if (running_.load(std::memory_order_acquire)) {
        return Status::Ok();
    }

    if (io_thread_.joinable()) {
        io_thread_.join();
    }
    thread_pool_.Stop();
    CloseListenFd();
    CloseAllClientFds();
    io_.reset();

    io_ = std::make_unique<CoroutineIoContext>();
    if (!io_->valid()) {
        const int error = io_->initialization_error();
        io_.reset();
        return Status::Error(
            StatusCode::kNetworkError,
            LastSocketError("coroutine IO context initialization failed", error));
    }

    endpoint_ = endpoint;
    Status status = SetupListener();
    if (!status.ok()) {
        CloseListenFd();
        io_.reset();
        return status;
    }

    thread_pool_.Start();
    {
        std::lock_guard<std::mutex> drain_lock(drain_mutex_);
        state_.store(State::kRunning, std::memory_order_release);
        running_.store(true, std::memory_order_release);
    }
    metrics_.SetServerState(static_cast<uint64_t>(State::kRunning));
    metrics_.SetShutdownStartTimeMs(0);

    CoroutineIoContext* const io = io_.get();
    io->Spawn([this] { AcceptLoop(); });
    io_running_.store(true, std::memory_order_release);
    io_thread_ = std::thread([this, io] {
        io->Run();
        io_running_.store(false, std::memory_order_release);
        if (running_.exchange(false, std::memory_order_acq_rel)) {
            CloseListenFd();
            CloseAllClientFds();
            {
                std::lock_guard<std::mutex> lock(drain_mutex_);
                state_.store(State::kStopped, std::memory_order_release);
            }
            metrics_.SetServerState(static_cast<uint64_t>(State::kStopped));
            drain_cv_.notify_all();
        }
    });
    return Status::Ok();
}

void CoroutineRpcServer::Stop() {
    Stop(kDefaultStopGrace);
}

void CoroutineRpcServer::Stop(std::chrono::milliseconds grace_period) {
    std::lock_guard<std::mutex> operation_lock(operation_mutex_);
    if (state_.load(std::memory_order_acquire) == State::kStopped) {
        if (io_thread_.joinable()) {
            io_thread_.join();
        }
        thread_pool_.Stop();
        CloseListenFd();
        CloseAllClientFds();
        io_.reset();
        return;
    }

    {
        std::lock_guard<std::mutex> drain_lock(drain_mutex_);
        state_.store(State::kDraining, std::memory_order_release);
        running_.store(false, std::memory_order_release);
    }
    metrics_.SetServerState(static_cast<uint64_t>(State::kDraining));
    metrics_.SetShutdownStartTimeMs(UnixTimeMs());

    (void)PostIoAndWait([this] { CloseListenFd(); });
    if (!WaitForPendingRequests(grace_period)) {
        metrics_.RecordGracefulShutdownTimeout();
    }
    (void)PostIoAndWait([this] { CloseAllClientFds(); });
    if (io_ != nullptr) {
        io_->Stop();
    }
    if (io_thread_.joinable()) {
        io_thread_.join();
    }
    CloseListenFd();
    CloseAllClientFds();
    thread_pool_.Stop();
    io_.reset();
    {
        std::lock_guard<std::mutex> drain_lock(drain_mutex_);
        state_.store(State::kStopped, std::memory_order_release);
    }
    metrics_.SetServerState(static_cast<uint64_t>(State::kStopped));
    drain_cv_.notify_all();
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
        if (!io_->WaitReadable(listen_fd_)) {
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
            io_->Spawn([this, client_fd] {
                try {
                    CoroutineRpcConnection connection(
                        io_.get(),
                        &registry_,
                        &thread_pool_,
                        &metrics_,
                        [this] { return TryAdmitRequest(); },
                    [this] { CompletePendingRequest(); });
                    (void)connection.Serve(client_fd);
                } catch (...) {
                    // Keep an unexpected exception from bypassing tracked fd cleanup.
                }
                CloseClientFd(client_fd);
            });
        }
    }
}

bool CoroutineRpcServer::TryAdmitRequest() {
    std::lock_guard<std::mutex> lock(drain_mutex_);
    if (state_.load(std::memory_order_acquire) != State::kRunning) {
        return false;
    }
    metrics_.IncrementPendingRequests();
    return true;
}

void CoroutineRpcServer::CompletePendingRequest() {
    {
        std::lock_guard<std::mutex> lock(drain_mutex_);
        metrics_.DecrementPendingRequests();
    }
    drain_cv_.notify_all();
}

void CoroutineRpcServer::TrackClientFd(int fd) {
    if (client_fds_.insert(fd).second) {
        metrics_.IncrementActiveConnections();
    }
}

void CoroutineRpcServer::CloseClientFd(int fd) {
    if (client_fds_.erase(fd) > 0) {
        ::close(fd);
        metrics_.DecrementActiveConnections();
    }
}

void CoroutineRpcServer::CloseAllClientFds() {
    for (int fd : client_fds_) {
        ::close(fd);
        metrics_.DecrementActiveConnections();
    }
    client_fds_.clear();
}

void CoroutineRpcServer::CloseListenFd() {
    if (listen_fd_ != -1) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

bool CoroutineRpcServer::WaitForPendingRequests(std::chrono::milliseconds grace_period) {
    std::unique_lock<std::mutex> lock(drain_mutex_);
    const auto drained = [this] { return metrics_.pending_requests() == 0; };
    if (grace_period <= std::chrono::milliseconds::zero()) {
        return drained();
    }
    return drain_cv_.wait_for(lock, grace_period, drained);
}

bool CoroutineRpcServer::PostIoAndWait(CoroutineIoContext::Task task) {
    if (io_ == nullptr || !io_running_.load(std::memory_order_acquire)) {
        return false;
    }

    auto completed = std::make_shared<std::promise<void>>();
    std::future<void> future = completed->get_future();
    if (!io_->Post([task = std::move(task), completed] {
            task();
            completed->set_value();
        })) {
        return false;
    }
    while (future.wait_for(std::chrono::milliseconds(1)) != std::future_status::ready) {
        if (!io_running_.load(std::memory_order_acquire)) {
            return false;
        }
    }
    return true;
}

}  // namespace minirpc
