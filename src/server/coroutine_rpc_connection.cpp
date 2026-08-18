#include "minirpc/server/coroutine_rpc_connection.h"

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
    bool waiting = false;
    bool done = false;
    RpcResponse response;
};

}  // namespace

CoroutineRpcConnection::CoroutineRpcConnection(CoroutineIoContext* io,
                                               ServiceRegistry* registry,
                                               ThreadPool* thread_pool,
                                               RpcMetrics* metrics,
                                               RequestAdmission request_admission,
                                               RequestCompletion request_completion)
    : io_(io),
      registry_(registry),
      thread_pool_(thread_pool),
      metrics_(metrics),
      request_admission_(std::move(request_admission)),
      request_completion_(std::move(request_completion)),
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
        }
        if (request_admission_ && !request_admission_()) {
            return RejectRequest(fd, frame.request_id, start_time);
        }
        if (!request_admission_ && metrics_ != nullptr) {
            metrics_->IncrementPendingRequests();
        }
        const uint64_t request_id = frame.request_id;
        RpcResponse response;
        try {
            response = thread_pool_ == nullptr
                           ? ProcessFrame(frame)
                           : ProcessFrameThroughThreadPool(std::move(frame));
            ProtocolFrame response_frame;
            status = PrepareResponseFrame(request_id, &response, &response_frame);
            if (status.ok()) {
                status = channel_.WriteFrame(fd, response_frame);
            }
        } catch (const std::exception& ex) {
            response = MakeErrorResponse(request_id, StatusCode::kServerError, ex.what());
            status = Status::Error(StatusCode::kServerError, ex.what());
        } catch (...) {
            response = MakeErrorResponse(request_id, StatusCode::kServerError, "unknown server error");
            status = Status::Error(StatusCode::kServerError, "unknown server error");
        }
        FinishRequestMetrics(response, status, start_time);
        if (!status.ok()) {
            return status;
        }
    }
}

Status CoroutineRpcConnection::RejectRequest(
    int fd,
    uint64_t request_id,
    std::chrono::steady_clock::time_point start_time) {
    RpcResponse response = MakeErrorResponse(
        request_id, StatusCode::kServerError, "server shutting down");
    ProtocolFrame response_frame;
    Status write_status = PrepareResponseFrame(request_id, &response, &response_frame);
    if (write_status.ok()) {
        write_status = channel_.WriteFrame(fd, response_frame);
    }
    if (metrics_ != nullptr) {
        metrics_->RecordRejected();
        if (write_status.ok()) {
            metrics_->RecordResponse();
        }
        metrics_->RecordLatency(std::chrono::steady_clock::now() - start_time);
    }
    return write_status;
}

Status CoroutineRpcConnection::PrepareResponseFrame(uint64_t request_id,
                                                     RpcResponse* response,
                                                     ProtocolFrame* frame) const {
    if (response == nullptr || frame == nullptr) {
        return Status::Error(StatusCode::kSerializeError, "invalid response output");
    }

    const auto encode = [this, request_id](const RpcResponse& value,
                                           ProtocolFrame* output) -> Status {
        try {
            ProtocolFrame encoded = MakeResponseFrame(request_id, value);
            if (encoded.body.size() > kDefaultMaxFrameBodySize) {
                return Status::Error(StatusCode::kSerializeError, "response body too large");
            }
            *output = std::move(encoded);
            return Status::Ok();
        } catch (const BodyCodecError& ex) {
            return Status::Error(StatusCode::kSerializeError, ex.what());
        } catch (const std::exception& ex) {
            return Status::Error(StatusCode::kSerializeError, ex.what());
        } catch (...) {
            return Status::Error(StatusCode::kSerializeError, "failed to serialize response body");
        }
    };

    Status status = encode(*response, frame);
    if (status.ok()) {
        return status;
    }
    *response = MakeErrorResponse(request_id, StatusCode::kSerializeError, status.message());
    return encode(*response, frame);
}

RpcResponse CoroutineRpcConnection::ProcessFrame(const ProtocolFrame& frame) {
    RpcResponse response;
    if (frame.codec_type != CodecType::kProtobuf) {
        return MakeErrorResponse(
            frame.request_id, StatusCode::kNotImplemented, "unsupported RPC codec");
    }
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
    Coroutine* current = Coroutine::Current();
    if (current == nullptr || !current->handle() || io_ == nullptr || thread_pool_ == nullptr) {
        return MakeErrorResponse(frame.request_id, StatusCode::kServerError, "invalid coroutine thread pool dispatch");
    }
    const CoroutineHandle coroutine = current->handle();

    const uint64_t request_id = frame.request_id;
    auto pending = std::make_shared<PendingResponse>();
    io_->AddExternalWait();
    if (!thread_pool_->Post([this, frame = std::move(frame), pending, coroutine] {
            RpcResponse response = ProcessFrame(frame);
            bool should_schedule = false;
            {
                std::lock_guard<std::mutex> lock(pending->mutex);
                pending->response = std::move(response);
                pending->done = true;
                should_schedule = pending->waiting;
            }
            if (should_schedule) {
                io_->Schedule(coroutine);
            }
            io_->CompleteExternalWait();
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
                pending->waiting = false;
                return std::move(pending->response);
            }
            pending->waiting = true;
        }
        Scheduler::SuspendCurrent();
        {
            std::lock_guard<std::mutex> lock(pending->mutex);
            pending->waiting = false;
        }
    }
}

void CoroutineRpcConnection::FinishRequestMetrics(const RpcResponse& response,
                                                  const Status& write_status,
                                                  std::chrono::steady_clock::time_point start_time) {
    if (metrics_ == nullptr) {
        CompletePendingRequest();
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
    CompletePendingRequest();
}

void CoroutineRpcConnection::CompletePendingRequest() {
    if (request_completion_) {
        request_completion_();
    } else if (metrics_ != nullptr) {
        metrics_->DecrementPendingRequests();
    }
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
