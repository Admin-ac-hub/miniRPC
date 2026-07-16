#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

#include "minirpc/core/endpoint.h"
#include "minirpc/core/status.h"
#include "minirpc/net/tcp_connection.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"

namespace minirpc {

class TcpClient {
public:
    enum class CloseReason {
        kPeerClosed,
        kIoError,
        kProtocolError,
        kLocalClose,
    };

    using OnFrameFn = std::function<void(ProtocolFrame)>;
    using OnCloseFn = std::function<void(CloseReason, const std::string& message)>;

    TcpClient();
    ~TcpClient();

    TcpClient(const TcpClient&)            = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    void SetOnFrame(OnFrameFn fn);
    void SetOnClose(OnCloseFn fn);

    Status Connect(const Endpoint& endpoint);
    void   Close();

    Status SendFrame(const ProtocolFrame& frame);

    ConnectionState state() const;

private:
    void ReaderLoop();
    void FireClose(CloseReason reason, const std::string& message);

    RpcCodec codec_;
    OnFrameFn on_frame_;
    OnCloseFn on_close_;

    std::atomic<bool> closing_;
    std::atomic<bool> close_fired_;
    int fd_;
    std::thread reader_thread_;
    mutable std::mutex state_mutex_;
    std::mutex send_mutex_;
    std::mutex connect_mutex_;
    std::atomic<ConnectionState> state_;
};

}  // namespace minirpc
