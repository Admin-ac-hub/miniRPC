#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "minirpc/core/endpoint.h"
#include "minirpc/core/status.h"
#include "minirpc/observability/metrics.h"
#include "minirpc/runtime/coroutine_io_context.h"
#include "minirpc/runtime/thread_pool.h"
#include "minirpc/server/service_registry.h"

namespace minirpc {

class CoroutineRpcServer {
public:
    CoroutineRpcServer(std::size_t worker_count = 4, std::size_t max_queue_size = 10000);
    ~CoroutineRpcServer();

    CoroutineRpcServer(const CoroutineRpcServer&) = delete;
    CoroutineRpcServer& operator=(const CoroutineRpcServer&) = delete;

    void RegisterService(const std::string& service_name,
                         const std::string& method_name,
                         RpcHandler handler);

    Status Start(const Endpoint& endpoint);
    void Stop();
    void Stop(std::chrono::milliseconds grace_period);
    bool running() const;
    const RpcMetrics& metrics() const;
    std::size_t threadpool_queue_size() const;
    std::string MetricsText() const;

private:
    Status SetupListener();
    void AcceptLoop();
    void TrackClientFd(int fd);
    void CloseClientFd(int fd);
    void CloseAllClientFds();
    void CloseListenFd();
    void WaitForPendingRequests(std::chrono::milliseconds grace_period) const;

    std::atomic<bool> running_;
    Endpoint endpoint_;
    ServiceRegistry registry_;
    ThreadPool thread_pool_;
    RpcMetrics metrics_;
    CoroutineIoContext io_;
    std::thread io_thread_;
    int listen_fd_;
    std::mutex clients_mutex_;
    std::unordered_set<int> client_fds_{};
};

}  // namespace minirpc
