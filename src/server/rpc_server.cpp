#include "minirpc/server/rpc_server.h"

#include <chrono>
#include <exception>
#include <utility>

#include "minirpc/net/tcp_server_backend.h"
#include "minirpc/protocol/body_codec.h"

namespace minirpc {
namespace {

constexpr auto kDefaultStopGrace = std::chrono::milliseconds(1000);

uint64_t UnixTimeMs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
}

bool DeadlineExpired(const RpcRequest& request) {
    if (request.deadline_unix_ms <= 0) {
        return false;
    }
    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    return now_ms >= request.deadline_unix_ms;
}

RpcResponse MakeErrorResponse(uint64_t request_id, StatusCode code, const std::string& message) {
    RpcResponse response;
    response.request_id = request_id;
    response.status_code = static_cast<int32_t>(code);
    response.error_message = message;
    return response;
}

ProtocolFrame MakeResponseFrame(uint64_t request_id, const RpcResponse& response) {
    ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = MessageType::kResponse;
    frame.codec_type = CodecType::kProtobuf;
    frame.body = EncodeResponseBody(response);
    return frame;
}

Status EncodeResponseFrame(uint64_t request_id,
                           const RpcResponse& response,
                           ProtocolFrame* frame) {
    try {
        ProtocolFrame encoded = MakeResponseFrame(request_id, response);
        if (encoded.body.size() > kDefaultMaxFrameBodySize) {
            return Status::Error(StatusCode::kSerializeError, "response body too large");
        }
        *frame = std::move(encoded);
        return Status::Ok();
    } catch (const BodyCodecError& ex) {
        return Status::Error(StatusCode::kSerializeError, ex.what());
    } catch (const std::exception& ex) {
        return Status::Error(StatusCode::kSerializeError, ex.what());
    } catch (...) {
        return Status::Error(StatusCode::kSerializeError, "failed to serialize response body");
    }
}

Status PrepareResponseFrame(uint64_t request_id,
                            RpcResponse* response,
                            ProtocolFrame* frame) {
    Status status = EncodeResponseFrame(request_id, *response, frame);
    if (status.ok()) {
        return status;
    }
    *response = MakeErrorResponse(request_id, StatusCode::kSerializeError, status.message());
    return EncodeResponseFrame(request_id, *response, frame);
}

}  // namespace

RpcServer::RpcServer()
    : running_(false),
      shutdown_state_(ShutdownState::kStopped),
      thread_pool_(4, 10000) {
    metrics_.SetServerState(static_cast<uint64_t>(ShutdownState::kStopped));
    tcp_server_.SetOnOpen([this](ConnectionId, uint64_t) {
        metrics_.IncrementActiveConnections();
    });
    tcp_server_.SetOnFrame([this](ConnectionId id, uint64_t gen, ProtocolFrame frame) {
        OnFrame(id, gen, std::move(frame));
    });
    tcp_server_.SetOnClose([this](ConnectionId, uint64_t) {
        metrics_.DecrementActiveConnections();
    });
    tcp_server_.SetOnBackpressure([this](ConnectionId,
                                         uint64_t,
                                         int,
                                         bool in_backpressure,
                                         std::size_t write_buffer_size) {
        if (in_backpressure) {
            metrics_.EnterBackpressure();
        } else {
            metrics_.LeaveBackpressure();
        }
        metrics_.UpdateMaxWriteBufferSize(write_buffer_size);
    });
}

RpcServer::~RpcServer() { Stop(); }

void RpcServer::RegisterService(const std::string& service_name,
                                const std::string& method_name,
                                RpcHandler handler) {
    registry_.Register(service_name, method_name, std::move(handler));
}

Status RpcServer::Start(const Endpoint& endpoint) {
    if (running_.load(std::memory_order_acquire)) return Status::Ok();
    thread_pool_.Start();
    Status status = tcp_server_.Start(endpoint);
    if (!status.ok()) {
        thread_pool_.Stop();
        shutdown_state_.store(ShutdownState::kStopped, std::memory_order_release);
        metrics_.SetServerState(static_cast<uint64_t>(ShutdownState::kStopped));
        return status;
    }
    {
        std::lock_guard<std::mutex> lock(drain_mutex_);
        shutdown_state_.store(ShutdownState::kRunning, std::memory_order_release);
        running_.store(true, std::memory_order_release);
        response_drain_sealed_ = false;
    }
    metrics_.SetServerState(static_cast<uint64_t>(ShutdownState::kRunning));
    metrics_.SetShutdownStartTimeMs(0);
    return Status::Ok();
}

void RpcServer::Stop() {
    Stop(kDefaultStopGrace);
}

void RpcServer::Stop(std::chrono::milliseconds grace_period) {
    {
        std::unique_lock<std::mutex> lock(drain_mutex_);
        ShutdownState expected = ShutdownState::kRunning;
        if (!shutdown_state_.compare_exchange_strong(expected,
                                                     ShutdownState::kDraining,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire)) {
            drain_cv_.wait(lock, [this] {
                return shutdown_state_.load(std::memory_order_acquire) !=
                       ShutdownState::kDraining;
            });
            return;
        }
        running_.store(false, std::memory_order_release);
    }
    metrics_.SetServerState(static_cast<uint64_t>(ShutdownState::kDraining));
    metrics_.SetShutdownStartTimeMs(UnixTimeMs());
    tcp_server_.StopAccepting();
    if (!WaitForPendingRequests(grace_period)) {
        metrics_.RecordGracefulShutdownTimeout();
    }
    tcp_server_.Stop();
    thread_pool_.Stop();
    {
        std::lock_guard<std::mutex> lock(drain_mutex_);
        metrics_.SetServerState(static_cast<uint64_t>(ShutdownState::kStopped));
        shutdown_state_.store(ShutdownState::kStopped, std::memory_order_release);
        drain_cv_.notify_all();
    }
}

bool RpcServer::running() const {
    return running_.load(std::memory_order_acquire);
}

ShutdownState RpcServer::shutdown_state() const {
    return shutdown_state_.load(std::memory_order_acquire);
}

const RpcMetrics& RpcServer::metrics() const {
    return metrics_;
}

std::size_t RpcServer::threadpool_queue_size() const {
    return thread_pool_.queued_tasks();
}

std::string RpcServer::MetricsText() const {
    return metrics_.ToPrometheusText(threadpool_queue_size());
}

void RpcServer::OnFrame(ConnectionId conn_id, uint64_t generation, ProtocolFrame frame) {
    if (frame.message_type != MessageType::kRequest) {
        tcp_server_.CloseConnection(conn_id, generation);
        return;
    }

    const auto start_time = std::chrono::steady_clock::now();
    const uint64_t request_id = frame.request_id;
    metrics_.RecordRequest();
    bool admitted = false;
    bool track_rejection = false;
    {
        std::lock_guard<std::mutex> lock(drain_mutex_);
        const ShutdownState state = shutdown_state_.load(std::memory_order_acquire);
        if (state == ShutdownState::kRunning) {
            metrics_.IncrementPendingRequests();
            admitted = true;
        } else if (state == ShutdownState::kDraining && !response_drain_sealed_) {
            metrics_.IncrementPendingRequests();
            track_rejection = true;
        }
    }
    if (!admitted) {
        if (!track_rejection) {
            tcp_server_.CloseConnection(conn_id, generation);
            metrics_.RecordRejected();
            metrics_.RecordLatency(std::chrono::steady_clock::now() - start_time);
            return;
        }

        RpcResponse response = MakeErrorResponse(
            request_id, StatusCode::kServerError, "server shutting down");
        const auto finish_rejection = [this, start_time](Status send_status) {
            if (send_status.ok()) {
                metrics_.RecordResponse();
            }
            metrics_.RecordLatency(std::chrono::steady_clock::now() - start_time);
            CompletePendingRequest();
        };
        ProtocolFrame response_frame;
        const Status encode_status = PrepareResponseFrame(request_id, &response, &response_frame);
        metrics_.RecordRejected();
        if (!encode_status.ok()) {
            tcp_server_.CloseConnection(conn_id, generation);
            finish_rejection(encode_status);
            return;
        }
        const Status send_status = TcpServerInternalAccess::SendFrame(
            tcp_server_,
            conn_id,
            generation,
            response_frame,
            false,
            finish_rejection);
        if (!send_status.ok()) {
            finish_rejection(send_status);
        }
        return;
    }

    if (!thread_pool_.Post([this, conn_id, generation, start_time, f = std::move(frame)]() mutable {
            ProcessRequest(conn_id, generation, std::move(f), start_time);
        })) {
        RpcResponse response = MakeErrorResponse(
            request_id, StatusCode::kServerError, "thread pool queue is full");
        const auto finish_rejection = [this, start_time](Status send_status) {
            if (send_status.ok()) {
                metrics_.RecordResponse();
            }
            metrics_.RecordLatency(std::chrono::steady_clock::now() - start_time);
            CompletePendingRequest();
        };
        ProtocolFrame response_frame;
        const Status encode_status = PrepareResponseFrame(request_id, &response, &response_frame);
        metrics_.RecordRejected();
        if (!encode_status.ok()) {
            tcp_server_.CloseConnection(conn_id, generation);
            finish_rejection(encode_status);
            return;
        }
        const Status send_status = TcpServerInternalAccess::SendFrame(
            tcp_server_,
            conn_id,
            generation,
            response_frame,
            true,
            finish_rejection);
        if (!send_status.ok()) {
            finish_rejection(send_status);
        }
    }
}

void RpcServer::ProcessRequest(ConnectionId conn_id,
                               uint64_t generation,
                               ProtocolFrame frame,
                               std::chrono::steady_clock::time_point start_time) {
    RpcResponse response;
    try {
        RpcRequest request = DecodeRequestBody(frame.request_id, frame.body);
        if ((request.service_name == "rpc" && request.method_name == "metrics") ||
            request.service_name == "rpc.metrics") {
            response.request_id = request.request_id;
            response.status_code = static_cast<int32_t>(StatusCode::kOk);
            response.payload = MetricsText();
        } else if (DeadlineExpired(request)) {
            response = MakeErrorResponse(
                request.request_id, StatusCode::kTimeout, "request deadline exceeded");
        } else {
            RpcHandler handler = registry_.Find(request.service_name, request.method_name);
            if (!handler) {
                if (!registry_.HasService(request.service_name)) {
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
    ProtocolFrame response_frame;
    const Status encode_status = PrepareResponseFrame(frame.request_id, &response, &response_frame);
    if (!encode_status.ok()) {
        tcp_server_.CloseConnection(conn_id, generation);
        FinishRequestMetrics(response.status_code, encode_status, start_time);
        return;
    }
    const int32_t response_status_code = response.status_code;
    const auto finish_request = [this, response_status_code, start_time](Status send_status) {
        FinishRequestMetrics(response_status_code, send_status, start_time);
    };
    const Status send_status = TcpServerInternalAccess::SendFrame(
        tcp_server_,
        conn_id,
        generation,
        response_frame,
        false,
        finish_request);
    if (!send_status.ok()) {
        finish_request(send_status);
    }
}

void RpcServer::FinishRequestMetrics(int32_t response_status_code,
                                     const Status& send_status,
                                     std::chrono::steady_clock::time_point start_time) {
    if (!send_status.ok()) {
        metrics_.RecordFailure();
    } else {
        metrics_.RecordResponse();
        if (response_status_code == static_cast<int32_t>(StatusCode::kOk)) {
            metrics_.RecordSuccess();
        } else if (response_status_code == static_cast<int32_t>(StatusCode::kTimeout)) {
            metrics_.RecordTimeout();
        } else {
            metrics_.RecordFailure();
        }
    }
    metrics_.RecordLatency(std::chrono::steady_clock::now() - start_time);
    CompletePendingRequest();
}

void RpcServer::CompletePendingRequest() {
    {
        std::lock_guard<std::mutex> lock(drain_mutex_);
        metrics_.DecrementPendingRequests();
    }
    drain_cv_.notify_all();
}

bool RpcServer::WaitForPendingRequests(std::chrono::milliseconds grace_period) {
    std::unique_lock<std::mutex> lock(drain_mutex_);
    const auto drained = [this] { return metrics_.pending_requests() == 0; };
    bool all_requests_drained = false;
    if (grace_period <= std::chrono::milliseconds::zero()) {
        all_requests_drained = drained();
    } else {
        all_requests_drained = drain_cv_.wait_for(lock, grace_period, drained);
    }
    response_drain_sealed_ = true;
    return all_requests_drained;
}

}  // namespace minirpc
