#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

#include "minirpc/core/endpoint.h"
#include "minirpc/core/status.h"
#include "minirpc/net/connection.h"
#include "minirpc/net/tcp_server.h"
#include "minirpc/observability/metrics.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/runtime/thread_pool.h"
#include "minirpc/server/service_registry.h"

namespace minirpc {

enum class ShutdownState : uint64_t {
    kRunning = 0,
    kDraining = 1,
    kStopped = 2,
};

class RpcServer {
public:
    RpcServer();
    ~RpcServer();

    RpcServer(const RpcServer&)            = delete;
    RpcServer& operator=(const RpcServer&) = delete;

    void RegisterService(const std::string& service_name,
                         const std::string& method_name,
                         RpcHandler handler);

    Status Start(const Endpoint& endpoint);
    void   Stop();
    void   Stop(std::chrono::milliseconds grace_period);
    bool   running() const;
    ShutdownState shutdown_state() const;
    const RpcMetrics& metrics() const;
    std::size_t threadpool_queue_size() const;
    std::string MetricsText() const;

private:
    void OnFrame(ConnectionId conn_id, uint64_t generation, ProtocolFrame frame);
    void ProcessRequest(ConnectionId conn_id,
                        uint64_t generation,
                        ProtocolFrame frame,
                        std::chrono::steady_clock::time_point start_time);
    void FinishRequestMetrics(int32_t response_status_code,
                              const Status& send_status,
                              std::chrono::steady_clock::time_point start_time);
    void CompletePendingRequest();
    bool WaitForPendingRequests(std::chrono::milliseconds grace_period);

    std::atomic<bool> running_;
    std::atomic<ShutdownState> shutdown_state_;
    ServiceRegistry registry_;
    ThreadPool thread_pool_;
    TcpServer tcp_server_;
    RpcMetrics metrics_{};
    std::mutex drain_mutex_;
    std::condition_variable drain_cv_;
    bool response_drain_sealed_ = true;
};

}  // namespace minirpc
