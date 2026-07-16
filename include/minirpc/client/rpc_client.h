#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "minirpc/core/endpoint.h"
#include "minirpc/core/status.h"
#include "minirpc/net/tcp_client.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/protocol/message.h"

namespace minirpc {

struct RpcClientOptions {
    std::size_t max_reconnect_attempts = 1;
    std::chrono::milliseconds reconnect_interval{50};
};

class RpcClient {
public:
    explicit RpcClient(Endpoint endpoint);
    ~RpcClient();

    RpcClient(const RpcClient&)            = delete;
    RpcClient& operator=(const RpcClient&) = delete;

    Status Connect();
    void Close();
    void SetOptions(const RpcClientOptions& options);

    RpcResponse Call(const std::string& service_name,
                     const std::string& method_name,
                     const std::string& payload,
                     std::chrono::milliseconds timeout);
    std::future<RpcResponse> CallAsync(const std::string& service_name,
                                       const std::string& method_name,
                                       const std::string& payload,
                                       std::chrono::milliseconds timeout);

private:
    struct PendingCall {
        std::promise<RpcResponse> promise;
        std::chrono::steady_clock::time_point expire_time;
        std::atomic<bool> done{false};

        bool TryComplete(RpcResponse response) {
            bool expected = false;
            if (!done.compare_exchange_strong(expected, true,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
                return false;
            }
            promise.set_value(std::move(response));
            return true;
        }
    };

    RpcClientOptions OptionsSnapshot() const;
    Status ConnectWithRetry(std::chrono::steady_clock::time_point expire_time);
    void OnFrame(ProtocolFrame frame);
    void OnClose(TcpClient::CloseReason reason, const std::string& message);
    void FailPending(StatusCode code, const std::string& message);
    void TimeoutCleanerLoop();
    RpcResponse ErrorResponse(uint64_t request_id, StatusCode code, const std::string& message) const;

    Endpoint endpoint_;
    TcpClient tcp_client_;
    mutable std::mutex options_mutex_;
    RpcClientOptions options_;
    std::atomic<uint64_t> next_request_id_;
    std::atomic<bool> stopping_;
    std::thread timeout_thread_;
    std::mutex pending_mutex_;
    std::condition_variable pending_cv_;
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending_;
};

class RpcClientPool {
public:
    RpcClientPool(Endpoint endpoint, std::size_t size, RpcClientOptions options = {});

    Status Connect();
    void Close();

    RpcResponse Call(const std::string& service_name,
                     const std::string& method_name,
                     const std::string& payload,
                     std::chrono::milliseconds timeout);
    std::future<RpcResponse> CallAsync(const std::string& service_name,
                                       const std::string& method_name,
                                       const std::string& payload,
                                       std::chrono::milliseconds timeout);

private:
    RpcClient& NextClient();

    std::vector<std::unique_ptr<RpcClient>> clients_;
    std::atomic<std::size_t> next_index_{0};
};

}  // namespace minirpc
