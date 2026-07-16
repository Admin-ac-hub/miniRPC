#include "minirpc/server/rpc_server.h"

#include <chrono>
#include <exception>
#include <thread>
#include <utility>

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

ProtocolFrame MakeResponseFrame(uint64_t request_id, const RpcResponse& response) {
    ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = MessageType::kResponse;
    frame.codec_type = CodecType::kProtobuf;
    frame.body = EncodeResponseBody(response);
    return frame;
}

RpcResponse MakeErrorResponse(uint64_t request_id, StatusCode code, const std::string& message) {
    RpcResponse response;
    response.request_id = request_id;
    response.status_code = static_cast<int32_t>(code);
    response.error_message = message;
    return response;
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
    shutdown_state_.store(ShutdownState::kRunning, std::memory_order_release);
    metrics_.SetServerState(static_cast<uint64_t>(ShutdownState::kRunning));
    metrics_.SetShutdownStartTimeMs(0);
    running_.store(true, std::memory_order_release);
    return Status::Ok();
}

void RpcServer::Stop() {
    Stop(kDefaultStopGrace);
}

void RpcServer::Stop(std::chrono::milliseconds grace_period) {
    ShutdownState expected = ShutdownState::kRunning;
    if (!shutdown_state_.compare_exchange_strong(expected,
                                                 ShutdownState::kDraining,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
        while (shutdown_state_.load(std::memory_order_acquire) == ShutdownState::kDraining) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return;
    }
    metrics_.SetServerState(static_cast<uint64_t>(ShutdownState::kDraining));
    metrics_.SetShutdownStartTimeMs(UnixTimeMs());
    running_.store(false, std::memory_order_release);
    tcp_server_.StopAccepting();
    if (!WaitForPendingRequests(grace_period)) {
        metrics_.RecordGracefulShutdownTimeout();
    }
    tcp_server_.Stop();
    thread_pool_.Stop();
    shutdown_state_.store(ShutdownState::kStopped, std::memory_order_release);
    metrics_.SetServerState(static_cast<uint64_t>(ShutdownState::kStopped));
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
    if (shutdown_state_.load(std::memory_order_acquire) != ShutdownState::kRunning) {
        metrics_.RecordRequest();
        RpcResponse response = MakeErrorResponse(
            request_id, StatusCode::kServerError, "server shutting down");
        const Status send_status = tcp_server_.SendFrame(
            conn_id, generation, MakeResponseFrame(request_id, response), false);
        if (send_status.ok()) {
            metrics_.RecordResponse();
        }
        metrics_.RecordRejected();
        metrics_.RecordLatency(std::chrono::steady_clock::now() - start_time);
        return;
    }

    metrics_.RecordRequest();
    metrics_.IncrementPendingRequests();

    if (!thread_pool_.Post([this, conn_id, generation, start_time, f = std::move(frame)]() mutable {
            ProcessRequest(conn_id, generation, std::move(f), start_time);
        })) {
        RpcResponse response = MakeErrorResponse(
            request_id, StatusCode::kServerError, "thread pool queue is full");
        const Status send_status = tcp_server_.SendFrame(conn_id, generation,
                                                         MakeResponseFrame(request_id, response), true);
        if (send_status.ok()) {
            metrics_.RecordResponse();
        }
        metrics_.RecordRejected();
        metrics_.RecordLatency(std::chrono::steady_clock::now() - start_time);
        metrics_.DecrementPendingRequests();
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
    const Status send_status = tcp_server_.SendFrame(conn_id, generation,
                                                     MakeResponseFrame(frame.request_id, response), false);
    FinishRequestMetrics(response, send_status, start_time);
}

void RpcServer::FinishRequestMetrics(const RpcResponse& response,
                                     const Status& send_status,
                                     std::chrono::steady_clock::time_point start_time) {
    if (!send_status.ok()) {
        metrics_.RecordFailure();
    } else {
        metrics_.RecordResponse();
        if (response.status_code == static_cast<int32_t>(StatusCode::kOk)) {
            metrics_.RecordSuccess();
        } else if (response.status_code == static_cast<int32_t>(StatusCode::kTimeout)) {
            metrics_.RecordTimeout();
        } else {
            metrics_.RecordFailure();
        }
    }
    metrics_.RecordLatency(std::chrono::steady_clock::now() - start_time);
    metrics_.DecrementPendingRequests();
}

bool RpcServer::WaitForPendingRequests(std::chrono::milliseconds grace_period) const {
    if (grace_period <= std::chrono::milliseconds::zero()) {
        return metrics_.pending_requests() == 0;
    }
    const auto deadline = std::chrono::steady_clock::now() + grace_period;
    while (metrics_.pending_requests() > 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return metrics_.pending_requests() == 0;
}

}  // namespace minirpc
