#pragma once

#include <chrono>
#include <functional>

#include "minirpc/core/status.h"
#include "minirpc/observability/metrics.h"
#include "minirpc/protocol/coroutine_frame_channel.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/protocol/message.h"
#include "minirpc/runtime/coroutine_io_context.h"
#include "minirpc/runtime/thread_pool.h"
#include "minirpc/server/service_registry.h"

namespace minirpc {

class CoroutineRpcConnection {
public:
    using RequestAdmission = std::function<bool()>;
    using RequestCompletion = std::function<void()>;

    CoroutineRpcConnection(CoroutineIoContext* io,
                           ServiceRegistry* registry,
                           ThreadPool* thread_pool = nullptr,
                           RpcMetrics* metrics = nullptr,
                           RequestAdmission request_admission = {},
                           RequestCompletion request_completion = {});

    CoroutineRpcConnection(const CoroutineRpcConnection&) = delete;
    CoroutineRpcConnection& operator=(const CoroutineRpcConnection&) = delete;

    Status Serve(int fd);

private:
    RpcResponse ProcessFrame(const ProtocolFrame& frame);
    RpcResponse ProcessFrameThroughThreadPool(ProtocolFrame frame);
    Status RejectRequest(int fd,
                         uint64_t request_id,
                         std::chrono::steady_clock::time_point start_time);
    Status PrepareResponseFrame(uint64_t request_id,
                                RpcResponse* response,
                                ProtocolFrame* frame) const;
    void FinishRequestMetrics(const RpcResponse& response,
                              const Status& write_status,
                              std::chrono::steady_clock::time_point start_time);
    void CompletePendingRequest();
    ProtocolFrame MakeResponseFrame(uint64_t request_id, const RpcResponse& response) const;
    RpcResponse MakeErrorResponse(uint64_t request_id, StatusCode code, const std::string& message) const;

    CoroutineIoContext* io_;
    ServiceRegistry* registry_;
    ThreadPool* thread_pool_;
    RpcMetrics* metrics_;
    RequestAdmission request_admission_;
    RequestCompletion request_completion_;
    CoroutineFrameChannel channel_;
};

}  // namespace minirpc
