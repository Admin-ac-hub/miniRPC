#include "minirpc/net/tcp_client.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

#include "minirpc/net/socket_utils.h"

namespace minirpc {
namespace {

constexpr std::size_t kReadChunkSize = 4096;

void CloseFd(int* fd) {
    if (*fd != -1) {
        ::close(*fd);
        *fd = -1;
    }
}

}  // namespace

TcpClient::TcpClient()
    : closing_(false),
      close_fired_(false),
      fd_(-1),
      state_(ConnectionState::kDisconnected) {}

TcpClient::~TcpClient() { Close(); }

void TcpClient::SetOnFrame(OnFrameFn fn) { on_frame_ = std::move(fn); }
void TcpClient::SetOnClose(OnCloseFn fn) { on_close_ = std::move(fn); }

Status TcpClient::Connect(const Endpoint& endpoint) {
    std::lock_guard<std::mutex> connect_lock(connect_mutex_);
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (fd_ != -1) return Status::Ok();
    }
    if (reader_thread_.joinable() && reader_thread_.get_id() != std::this_thread::get_id()) {
        reader_thread_.join();
    }

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("socket failed", errno));
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(endpoint.port);
    if (::inet_pton(AF_INET, endpoint.host.c_str(), &address.sin_addr) != 1) {
        ::close(fd);
        return Status::Error(StatusCode::kNetworkError, "invalid endpoint host: " + endpoint.host);
    }
    state_.store(ConnectionState::kConnecting, std::memory_order_release);
    while (true) {
        if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0) break;
        if (errno == EINTR) continue;
        const int err = errno;
        ::close(fd);
        state_.store(ConnectionState::kDisconnected, std::memory_order_release);
        return Status::Error(StatusCode::kNetworkError, LastSocketError("connect failed", err));
    }
    closing_.store(false, std::memory_order_release);
    close_fired_.store(false, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        fd_ = fd;
    }
    state_.store(ConnectionState::kConnected, std::memory_order_release);
    reader_thread_ = std::thread([this] { ReaderLoop(); });
    return Status::Ok();
}

void TcpClient::Close() {
    const bool was_closing = closing_.exchange(true, std::memory_order_acq_rel);
    {
        std::lock_guard<std::mutex> send_lock(send_mutex_);
        std::lock_guard<std::mutex> state_lock(state_mutex_);
        if (fd_ != -1) {
            (void)::shutdown(fd_, SHUT_RDWR);
        }
    }
    if (reader_thread_.joinable() && reader_thread_.get_id() != std::this_thread::get_id()) {
        reader_thread_.join();
    }
    {
        std::lock_guard<std::mutex> send_lock(send_mutex_);
        std::lock_guard<std::mutex> state_lock(state_mutex_);
        CloseFd(&fd_);
    }
    state_.store(ConnectionState::kDisconnected, std::memory_order_release);
    if (!was_closing) {
        FireClose(CloseReason::kLocalClose, "");
    }
}

Status TcpClient::SendFrame(const ProtocolFrame& frame) {
    const std::string data = codec_.Encode(frame);
    std::lock_guard<std::mutex> send_lock(send_mutex_);
    if (closing_.load(std::memory_order_acquire)) {
        return Status::Error(StatusCode::kNetworkError, "connection is closed");
    }
    int fd = -1;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        fd = fd_;
    }
    if (fd == -1) {
        return Status::Error(StatusCode::kNetworkError, "connection is closed");
    }
    const SendResult sent = SendAll(fd, data.data(), data.size());
    if (sent.status == SendStatus::kOk) return Status::Ok();
    if (sent.status == SendStatus::kWouldBlock) {
        return Status::Error(StatusCode::kNetworkError, "send would block");
    }
    return Status::Error(StatusCode::kNetworkError, LastSocketError("send failed", sent.error_code));
}

ConnectionState TcpClient::state() const {
    return state_.load(std::memory_order_acquire);
}

void TcpClient::ReaderLoop() {
    std::string buffer;
    char temp[kReadChunkSize];
    CloseReason reason = CloseReason::kPeerClosed;
    std::string message;

    while (!closing_.load(std::memory_order_acquire)) {
        int fd = -1;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            fd = fd_;
        }
        if (fd == -1) break;

        const ssize_t n = ::recv(fd, temp, sizeof(temp), 0);
        if (n > 0) {
            buffer.append(temp, static_cast<std::size_t>(n));
            bool decode_error = false;
            while (true) {
                ProtocolFrame frame;
                std::string error;
                const DecodeResult result = codec_.TryDecode(buffer, &frame, &error);
                if (result == DecodeResult::kNeedMoreData) break;
                if (result == DecodeResult::kProtocolError) {
                    reason = CloseReason::kProtocolError;
                    message = error;
                    decode_error = true;
                    break;
                }
                if (on_frame_) on_frame_(std::move(frame));
            }
            if (decode_error) break;
            continue;
        }
        if (n == 0) {
            reason = CloseReason::kPeerClosed;
            message = "";
            break;
        }
        if (errno == EINTR) continue;
        reason = CloseReason::kIoError;
        message = LastSocketError("recv failed", errno);
        break;
    }

    {
        std::lock_guard<std::mutex> send_lock(send_mutex_);
        std::lock_guard<std::mutex> lock(state_mutex_);
        CloseFd(&fd_);
    }
    state_.store(ConnectionState::kDisconnected, std::memory_order_release);
    if (closing_.load(std::memory_order_acquire)) {
        reason = CloseReason::kLocalClose;
        message.clear();
    }
    FireClose(reason, message);
}

void TcpClient::FireClose(CloseReason reason, const std::string& message) {
    const bool already = close_fired_.exchange(true, std::memory_order_acq_rel);
    if (already) return;
    if (on_close_) on_close_(reason, message);
}

}  // namespace minirpc
