#include "minirpc/server/rpc_server.h"

#include <exception>
#include <utility>

#include "minirpc/protocol/body_codec.h"

namespace minirpc {
namespace {

ProtocolFrame MakeResponseFrame(uint64_t request_id, const RpcResponse& response) {
    ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = MessageType::kResponse;
    frame.codec_type = CodecType::kRaw;
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
      thread_pool_(4, 10000) {
    tcp_server_.SetOnFrame([this](ConnectionId id, uint64_t gen, ProtocolFrame frame) {
        OnFrame(id, gen, std::move(frame));
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
        return status;
    }
    running_.store(true, std::memory_order_release);
    return Status::Ok();
}

void RpcServer::Stop() {
    running_.store(false, std::memory_order_release);
    tcp_server_.Stop();
    thread_pool_.Stop();
}

bool RpcServer::running() const {
    return running_.load(std::memory_order_acquire);
}

void RpcServer::OnFrame(ConnectionId conn_id, uint64_t generation, ProtocolFrame frame) {
    if (frame.message_type != MessageType::kRequest) {
        tcp_server_.CloseConnection(conn_id, generation);
        return;
    }
    const uint64_t request_id = frame.request_id;
    if (!thread_pool_.Post([this, conn_id, generation, f = std::move(frame)]() mutable {
            ProcessRequest(conn_id, generation, std::move(f));
        })) {
        RpcResponse response = MakeErrorResponse(
            request_id, StatusCode::kServerError, "thread pool queue is full");
        (void)tcp_server_.SendFrame(conn_id, generation,
                                    MakeResponseFrame(request_id, response), true);
    }
}

void RpcServer::ProcessRequest(ConnectionId conn_id, uint64_t generation, ProtocolFrame frame) {
    RpcResponse response;
    try {
        RpcRequest request = DecodeRequestBody(frame.request_id, frame.body);
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
    } catch (const BodyCodecError& ex) {
        response = MakeErrorResponse(frame.request_id, StatusCode::kDeserializeError, ex.what());
    } catch (const std::exception& ex) {
        response = MakeErrorResponse(frame.request_id, StatusCode::kServerError, ex.what());
    } catch (...) {
        response = MakeErrorResponse(frame.request_id, StatusCode::kServerError, "unknown server error");
    }
    (void)tcp_server_.SendFrame(conn_id, generation,
                                MakeResponseFrame(frame.request_id, response), false);
}

}  // namespace minirpc
