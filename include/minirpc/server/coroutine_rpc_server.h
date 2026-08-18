#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <condition_variable>
#include <memory>
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
    enum class State : uint64_t {
        kRunning = 0,
        kDraining = 1,
        kStopped = 2,
    };

    Status SetupListener();
    void AcceptLoop();
    bool TryAdmitRequest();
    void CompletePendingRequest();
    void TrackClientFd(int fd);
    void CloseClientFd(int fd);
    void CloseAllClientFds();
    void CloseListenFd();
    bool WaitForPendingRequests(std::chrono::milliseconds grace_period);
    bool PostIoAndWait(CoroutineIoContext::Task task);

    std::atomic<bool> running_;
    std::atomic<bool> io_running_;
    std::atomic<State> state_;
    Endpoint endpoint_;
    ServiceRegistry registry_;
    ThreadPool thread_pool_;
    RpcMetrics metrics_;
    std::unique_ptr<CoroutineIoContext> io_;
    std::thread io_thread_;
    int listen_fd_;
    std::mutex operation_mutex_;
    std::mutex drain_mutex_;
    std::condition_variable drain_cv_;
    std::unordered_set<int> client_fds_{};
};

}  // namespace minirpc
