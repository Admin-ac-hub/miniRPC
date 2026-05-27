#pragma once

#include <atomic>
#include <string>

#include "minirpc/core/endpoint.h"
#include "minirpc/core/status.h"
#include "minirpc/net/connection.h"
#include "minirpc/net/tcp_server.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/runtime/thread_pool.h"
#include "minirpc/server/service_registry.h"

namespace minirpc {

class RpcServer {
public:
    RpcServer();
    ~RpcServer();

    RpcServer(const RpcServer&)            = delete;
    RpcServer& operator=(const RpcServer&) = delete;

    void RegisterService(const std::string& service_name,
                         const std::string& method_name,
                         RpcHandler handler);

    Status Start(const Endpoint& endpoint);
    void   Stop();
    bool   running() const;

private:
    void OnFrame(ConnectionId conn_id, uint64_t generation, ProtocolFrame frame);
    void ProcessRequest(ConnectionId conn_id, uint64_t generation, ProtocolFrame frame);

    std::atomic<bool> running_;
    ServiceRegistry registry_;
    ThreadPool thread_pool_;
    TcpServer tcp_server_;
};

}  // namespace minirpc
