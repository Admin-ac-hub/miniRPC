#pragma once

#include <chrono>

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
    CoroutineRpcConnection(CoroutineIoContext* io,
                           ServiceRegistry* registry,
                           ThreadPool* thread_pool = nullptr,
                           RpcMetrics* metrics = nullptr);

    CoroutineRpcConnection(const CoroutineRpcConnection&) = delete;
    CoroutineRpcConnection& operator=(const CoroutineRpcConnection&) = delete;

    Status Serve(int fd);

private:
    RpcResponse ProcessFrame(const ProtocolFrame& frame);
    RpcResponse ProcessFrameThroughThreadPool(ProtocolFrame frame);
    void FinishRequestMetrics(const RpcResponse& response,
                              const Status& write_status,
                              std::chrono::steady_clock::time_point start_time);
    ProtocolFrame MakeResponseFrame(uint64_t request_id, const RpcResponse& response) const;
    RpcResponse MakeErrorResponse(uint64_t request_id, StatusCode code, const std::string& message) const;

    CoroutineIoContext* io_;
    ServiceRegistry* registry_;
    ThreadPool* thread_pool_;
    RpcMetrics* metrics_;
    CoroutineFrameChannel channel_;
};

}  // namespace minirpc
