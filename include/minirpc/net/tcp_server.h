#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "minirpc/core/endpoint.h"
#include "minirpc/core/status.h"
#include "minirpc/net/connection.h"
#include "minirpc/protocol/frame.h"

namespace minirpc {

class TcpServerInternalAccess;

struct TcpServerOptions {
    std::size_t max_connections        = 10000;
    std::size_t max_read_buffer_bytes  = 2 * 1024 * 1024;
    std::size_t max_write_buffer_bytes = 16 * 1024 * 1024;
    std::size_t high_watermark_bytes   = 1024 * 1024;
    std::size_t low_watermark_bytes    = 256 * 1024;
    std::size_t max_response_queue     = 10000;
    int backlog                        = 128;
};

class TcpServer {
public:
    using OnOpenFn = std::function<void(ConnectionId, uint64_t generation)>;
    using OnFrameFn = std::function<void(ConnectionId, uint64_t generation, ProtocolFrame)>;
    using OnCloseFn = std::function<void(ConnectionId, uint64_t generation)>;
    using OnBackpressureFn = std::function<void(ConnectionId,
                                                uint64_t generation,
                                                int fd,
                                                bool in_backpressure,
                                                std::size_t write_buffer_size)>;

    TcpServer();
    ~TcpServer();

    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void SetOnOpen(OnOpenFn fn);
    void SetOnFrame(OnFrameFn fn);
    void SetOnClose(OnCloseFn fn);
    void SetOnBackpressure(OnBackpressureFn fn);
    void SetOptions(const TcpServerOptions& opts);

    Status Start(const Endpoint& endpoint);
    void   Stop();
    void   StopAccepting();
    bool   running() const;

    Status SendFrame(ConnectionId conn_id,
                     uint64_t generation,
                     const ProtocolFrame& frame,
                     bool close_after_send);

    void CloseConnection(ConnectionId conn_id, uint64_t generation);

private:
    Status SendFrameWithCompletion(ConnectionId conn_id,
                                   uint64_t generation,
                                   const ProtocolFrame& frame,
                                   bool close_after_send,
                                   std::function<void(Status)> completion);

    friend class TcpServerInternalAccess;

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace minirpc
