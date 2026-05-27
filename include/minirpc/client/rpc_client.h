#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "minirpc/core/endpoint.h"
#include "minirpc/core/status.h"
#include "minirpc/net/tcp_client.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/protocol/message.h"

namespace minirpc {

class RpcClient {
public:
    explicit RpcClient(Endpoint endpoint);
    ~RpcClient();

    RpcClient(const RpcClient&)            = delete;
    RpcClient& operator=(const RpcClient&) = delete;

    Status Connect();
    void Close();

    RpcResponse Call(const std::string& service_name,
                     const std::string& method_name,
                     const std::string& payload,
                     std::chrono::milliseconds timeout);

private:
    void OnFrame(ProtocolFrame frame);
    void OnClose(TcpClient::CloseReason reason, const std::string& message);
    void FailPending(StatusCode code, const std::string& message);
    RpcResponse ErrorResponse(uint64_t request_id, StatusCode code, const std::string& message) const;

    Endpoint endpoint_;
    TcpClient tcp_client_;
    std::atomic<uint64_t> next_request_id_;
    std::mutex pending_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<std::promise<RpcResponse>>> pending_;
};

}  // namespace minirpc
