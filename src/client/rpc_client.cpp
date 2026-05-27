#include "minirpc/client/rpc_client.h"

#include <exception>
#include <utility>
#include <vector>

#include "minirpc/protocol/body_codec.h"

namespace minirpc {

RpcClient::RpcClient(Endpoint endpoint)
    : endpoint_(std::move(endpoint)),
      next_request_id_(1) {
    tcp_client_.SetOnFrame([this](ProtocolFrame frame) { OnFrame(std::move(frame)); });
    tcp_client_.SetOnClose([this](TcpClient::CloseReason r, const std::string& msg) { OnClose(r, msg); });
}

RpcClient::~RpcClient() { Close(); }

Status RpcClient::Connect() { return tcp_client_.Connect(endpoint_); }

void RpcClient::Close() { tcp_client_.Close(); }

RpcResponse RpcClient::Call(const std::string& service_name,
                            const std::string& method_name,
                            const std::string& payload,
                            std::chrono::milliseconds timeout) {
    const Status connected = Connect();
    const uint64_t request_id = next_request_id_.fetch_add(1, std::memory_order_relaxed);
    if (!connected.ok()) {
        return ErrorResponse(request_id, connected.code(), connected.message());
    }

    RpcRequest request;
    request.request_id = request_id;
    request.service_name = service_name;
    request.method_name = method_name;
    request.payload = payload;

    ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = MessageType::kRequest;
    frame.codec_type = CodecType::kRaw;
    try {
        frame.body = EncodeRequestBody(request);
    } catch (const std::exception& ex) {
        return ErrorResponse(request_id, StatusCode::kSerializeError, ex.what());
    }

    auto pending = std::make_shared<PendingCall>();
    auto future = pending->promise.get_future();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_[request_id] = pending;
    }

    Status send_status = tcp_client_.SendFrame(frame);
    if (!send_status.ok()) {
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_.erase(request_id);
        }
        pending->TryComplete(ErrorResponse(request_id, send_status.code(), send_status.message()));
        tcp_client_.Close();
        return future.get();
    }

    if (future.wait_for(timeout) != std::future_status::ready) {
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_.erase(request_id);
        }
        pending->TryComplete(ErrorResponse(request_id, StatusCode::kTimeout, "rpc call timed out"));
    }
    return future.get();
}

void RpcClient::OnFrame(ProtocolFrame frame) {
    if (frame.message_type != MessageType::kResponse) {
        FailPending(StatusCode::kProtocolError, "unexpected non-response frame");
        tcp_client_.Close();
        return;
    }
    RpcResponse response;
    try {
        response = DecodeResponseBody(frame.request_id, frame.body);
    } catch (const std::exception& ex) {
        response = ErrorResponse(frame.request_id, StatusCode::kDeserializeError, ex.what());
    }
    std::shared_ptr<PendingCall> pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_.find(frame.request_id);
        if (it != pending_.end()) {
            pending = std::move(it->second);
            pending_.erase(it);
        }
    }
    if (pending) pending->TryComplete(std::move(response));
}

void RpcClient::OnClose(TcpClient::CloseReason, const std::string& message) {
    FailPending(StatusCode::kNetworkError,
                message.empty() ? std::string("connection closed by peer") : message);
}

void RpcClient::FailPending(StatusCode code, const std::string& message) {
    std::vector<std::pair<uint64_t, std::shared_ptr<PendingCall>>> drained;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        drained.reserve(pending_.size());
        for (auto& kv : pending_) drained.emplace_back(kv.first, std::move(kv.second));
        pending_.clear();
    }
    for (auto& [request_id, pending] : drained) {
        pending->TryComplete(ErrorResponse(request_id, code, message));
    }
}

RpcResponse RpcClient::ErrorResponse(uint64_t request_id,
                                     StatusCode code,
                                     const std::string& message) const {
    RpcResponse response;
    response.request_id = request_id;
    response.status_code = static_cast<int32_t>(code);
    response.error_message = message;
    return response;
}

}  // namespace minirpc
