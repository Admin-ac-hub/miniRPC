#pragma once

#include <functional>
#include <memory>

#include "minirpc/net/tcp_server.h"

namespace minirpc {

class TcpServerBackend {
public:
    virtual ~TcpServerBackend() = default;

    virtual void SetOnOpen(TcpServer::OnOpenFn fn) = 0;
    virtual void SetOnFrame(TcpServer::OnFrameFn fn) = 0;
    virtual void SetOnClose(TcpServer::OnCloseFn fn) = 0;
    virtual void SetOnBackpressure(TcpServer::OnBackpressureFn fn) = 0;
    virtual void SetOptions(const TcpServerOptions& opts) = 0;

    virtual Status Start(const Endpoint& endpoint) = 0;
    virtual void Stop() = 0;
    virtual void StopAccepting() = 0;
    virtual bool running() const = 0;

    virtual Status SendFrame(ConnectionId conn_id,
                             uint64_t generation,
                             const ProtocolFrame& frame,
                             bool close_after_send,
                             std::function<void(Status)> completion) = 0;
    virtual void CloseConnection(ConnectionId conn_id, uint64_t generation) = 0;
};

class TcpServerInternalAccess {
public:
    static Status SendFrame(TcpServer& server,
                            ConnectionId conn_id,
                            uint64_t generation,
                            const ProtocolFrame& frame,
                            bool close_after_send,
                            std::function<void(Status)> completion);
};

std::unique_ptr<TcpServerBackend> MakeEpollTcpServerBackend();
std::unique_ptr<TcpServerBackend> MakeIoUringTcpServerBackend();

}  // namespace minirpc
