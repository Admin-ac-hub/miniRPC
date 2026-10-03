#pragma once

#include <chrono>
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
    // 连接无成功读写超过该时长即被服务端关闭；0 = 禁用（默认，保持既有行为）。
    std::chrono::milliseconds idle_timeout_ms{0};
    std::size_t max_frame_body_size = 2 * 1024 * 1024 - kProtocolHeaderSize;
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
    // 帧被拒（协议错误，或读缓冲在凑齐一帧前达到上限），连接即将关闭。
    // 返回 true 表示上层已接管关闭（例如排入一个带错误码的响应并设 close_after_send），
    // backend 不再直接关闭；返回 false 则由 backend 立即关闭。
    // request_id 为 0 表示头未解析到该字段（magic/version 不符，或头都不完整），不可用于构造响应。
    using OnFrameRejectedFn = std::function<bool(ConnectionId conn_id,
                                                 uint64_t generation,
                                                 uint64_t request_id,
                                                 const std::string& reason)>;

    TcpServer();
    ~TcpServer();

    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void SetOnOpen(OnOpenFn fn);
    void SetOnFrame(OnFrameFn fn);
    void SetOnClose(OnCloseFn fn);
    void SetOnBackpressure(OnBackpressureFn fn);
    // Configure before Start() or after Stop() has joined the reactor; otherwise ignored.
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
