#include "minirpc/server/coroutine_rpc_connection.h"

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <utility>

#include "minirpc/protocol/body_codec.h"

namespace minirpc {

namespace {

bool DeadlineExpired(const RpcRequest& request) {
    if (request.deadline_unix_ms <= 0) {
        return false;
    }
    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    return now_ms >= request.deadline_unix_ms;
}

struct PendingResponse {
    std::mutex mutex;
    std::atomic<bool> waiting{false};
    bool done = false;
    RpcResponse response;
};

}  // namespace

CoroutineRpcConnection::CoroutineRpcConnection(CoroutineIoContext* io,
                                               ServiceRegistry* registry,
                                               ThreadPool* thread_pool,
                                               RpcMetrics* metrics)
    : io_(io),
      registry_(registry),
      thread_pool_(thread_pool),
      metrics_(metrics),
      channel_(io) {}

Status CoroutineRpcConnection::Serve(int fd) {
    if (io_ == nullptr || registry_ == nullptr) {
        return Status::Error(StatusCode::kServerError, "invalid coroutine rpc connection");
    }

    while (true) {
        ProtocolFrame frame;
        Status status = channel_.ReadFrame(fd, &frame);
        if (!status.ok()) {
            return status.code() == StatusCode::kNetworkError ? Status::Ok() : status;
        }
        if (frame.message_type != MessageType::kRequest) {
            return Status::Error(StatusCode::kProtocolError, "unexpected non-request frame");
        }

        const auto start_time = std::chrono::steady_clock::now();
        if (metrics_ != nullptr) {
            metrics_->RecordRequest();
            metrics_->IncrementPendingRequests();
        }
        const uint64_t request_id = frame.request_id;
        RpcResponse response = thread_pool_ == nullptr
                                   ? ProcessFrame(frame)
                                   : ProcessFrameThroughThreadPool(std::move(frame));
        status = channel_.WriteFrame(fd, MakeResponseFrame(request_id, response));
        FinishRequestMetrics(response, status, start_time);
        if (!status.ok()) {
            return status;
        }
    }
}

RpcResponse CoroutineRpcConnection::ProcessFrame(const ProtocolFrame& frame) {
    RpcResponse response;
    try {
        RpcRequest request = DecodeRequestBody(frame.request_id, frame.body);
        if (DeadlineExpired(request)) {
            response = MakeErrorResponse(
                request.request_id, StatusCode::kTimeout, "request deadline exceeded");
        } else {
            RpcHandler handler = registry_->Find(request.service_name, request.method_name);
            if (!handler) {
                if (!registry_->HasService(request.service_name)) {
                    response = MakeErrorResponse(
                        request.request_id, StatusCode::kServiceNotFound, "service not found");
                } else {
                    response = MakeErrorResponse(
                        request.request_id, StatusCode::kMethodNotFound, "method not found");
                }
            } else {
                response = handler(request);
                response.request_id = request.request_id;
            }
        }
    } catch (const BodyCodecError& ex) {
        response = MakeErrorResponse(frame.request_id, StatusCode::kDeserializeError, ex.what());
    } catch (const std::exception& ex) {
        response = MakeErrorResponse(frame.request_id, StatusCode::kServerError, ex.what());
    } catch (...) {
        response = MakeErrorResponse(frame.request_id, StatusCode::kServerError, "unknown server error");
    }
    return response;
}

RpcResponse CoroutineRpcConnection::ProcessFrameThroughThreadPool(ProtocolFrame frame) {
    Coroutine* coroutine = Coroutine::Current();
    if (coroutine == nullptr || io_ == nullptr || thread_pool_ == nullptr) {
        return MakeErrorResponse(frame.request_id, StatusCode::kServerError, "invalid coroutine thread pool dispatch");
    }

    const uint64_t request_id = frame.request_id;
    auto pending = std::make_shared<PendingResponse>();
    io_->AddExternalWait();
    if (!thread_pool_->Post([this, frame = std::move(frame), pending, coroutine] {
            RpcResponse response = ProcessFrame(frame);
            {
                std::lock_guard<std::mutex> lock(pending->mutex);
                pending->response = std::move(response);
                pending->done = true;
            }
            io_->CompleteExternalWait();
            if (pending->waiting.load(std::memory_order_acquire)) {
                io_->Schedule(coroutine);
            }
    })) {
        io_->CompleteExternalWait();
        if (metrics_ != nullptr) {
            metrics_->RecordRejected();
        }
        return MakeErrorResponse(request_id, StatusCode::kServerError, "thread pool queue is full");
    }

    while (true) {
        {
            std::lock_guard<std::mutex> lock(pending->mutex);
            if (pending->done) {
                pending->waiting.store(false, std::memory_order_release);
                return std::move(pending->response);
            }
        }
        pending->waiting.store(true, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(pending->mutex);
            if (pending->done) {
                pending->waiting.store(false, std::memory_order_release);
                return std::move(pending->response);
            }
        }
        Scheduler::SuspendCurrent();
        pending->waiting.store(false, std::memory_order_release);
    }
}

void CoroutineRpcConnection::FinishRequestMetrics(const RpcResponse& response,
                                                  const Status& write_status,
                                                  std::chrono::steady_clock::time_point start_time) {
    if (metrics_ == nullptr) {
        return;
    }
    const bool already_rejected = response.error_message == "thread pool queue is full";
    if (write_status.ok()) {
        metrics_->RecordResponse();
    }
    if (already_rejected) {
        // Rejection was already counted when ThreadPool::Post failed.
    } else if (!write_status.ok()) {
        metrics_->RecordFailure();
    } else {
        if (response.status_code == static_cast<int32_t>(StatusCode::kOk)) {
            metrics_->RecordSuccess();
        } else if (response.status_code == static_cast<int32_t>(StatusCode::kTimeout)) {
            metrics_->RecordTimeout();
        } else {
            metrics_->RecordFailure();
        }
    }
    metrics_->RecordLatency(std::chrono::steady_clock::now() - start_time);
    metrics_->DecrementPendingRequests();
}

ProtocolFrame CoroutineRpcConnection::MakeResponseFrame(uint64_t request_id,
                                                        const RpcResponse& response) const {
    ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = MessageType::kResponse;
    frame.codec_type = CodecType::kProtobuf;
    frame.body = EncodeResponseBody(response);
    return frame;
}

RpcResponse CoroutineRpcConnection::MakeErrorResponse(uint64_t request_id,
                                                      StatusCode code,
                                                      const std::string& message) const {
    RpcResponse response;
    response.request_id = request_id;
    response.status_code = static_cast<int32_t>(code);
    response.error_message = message;
    return response;
}

}  // namespace minirpc
