#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "minirpc/core/endpoint.h"
#include "minirpc/core/status.h"
#include "minirpc/net/connection.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"

namespace minirpc {

struct TcpServerOptions {
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
    struct Connection {
        ConnectionId id = 0;
        int fd = -1;
        uint64_t generation = 0;
        std::string read_buffer;
        std::deque<std::string> write_buffers;
        std::size_t write_buffer_offset = 0;
        std::size_t write_buffer_bytes = 0;
        bool closing = false;
        bool in_backpressure = false;
    };

    struct QueuedResponse {
        ConnectionId conn_id = 0;
        uint64_t generation = 0;
        std::string data;
        bool close_after_send = false;
    };

    struct QueuedClose {
        ConnectionId conn_id = 0;
        uint64_t generation = 0;
    };

    Status SetupListener();
    void RunEventLoop();
    void AcceptConnections();
    void HandleClientRead(int client_fd);
    void DrainResponses();
    void DrainCloseRequests();
    void DrainWakeEvents();
    void WakeEventLoop();
    void CloseListenFd();
    Connection* FindByFd(int fd);
    Connection* FindById(ConnectionId conn_id);
    bool MatchesGeneration(ConnectionId conn_id, uint64_t generation) const;
    bool HasPendingWrites(const Connection& conn) const;
    void QueueWriteBuffer(Connection& conn, std::string&& data, std::size_t offset);
    bool SendToConnection(Connection& conn, std::string&& data);
    void FlushWriteBuffer(int client_fd);
    bool UpdateInterest(Connection& conn, bool want_write);
    bool SetBackpressure(Connection& conn, bool enabled);
    bool MaybeEnterBackpressure(Connection& conn);
    bool MaybeLeaveBackpressure(Connection& conn);
    void CloseByFd(int client_fd);
    void CloseById(ConnectionId conn_id);

    std::atomic<bool> running_;
    std::atomic<bool> accepting_;
    Endpoint endpoint_;
    TcpServerOptions options_;
    RpcCodec codec_;
    OnOpenFn on_open_;
    OnFrameFn on_frame_;
    OnCloseFn on_close_;
    OnBackpressureFn on_backpressure_;

    std::thread reactor_thread_;
    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    std::unordered_map<int, ConnectionId> connection_by_fd_;
    std::unordered_map<ConnectionId, Connection> connections_;
    ConnectionId next_connection_id_ = 1;
    uint64_t next_generation_ = 1;
    mutable std::mutex response_mutex_;
    std::vector<QueuedResponse> response_queue_;
    std::vector<QueuedClose> close_queue_;
};

}  // namespace minirpc
