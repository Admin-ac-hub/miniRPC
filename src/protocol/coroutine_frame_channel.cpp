#include "minirpc/protocol/coroutine_frame_channel.h"

#include <cerrno>

#include "minirpc/net/socket_utils.h"

namespace minirpc {
namespace {

constexpr std::size_t kReadChunkSize = 4096;

}  // namespace

CoroutineFrameChannel::CoroutineFrameChannel(CoroutineIoContext* io, std::size_t max_body_size)
    : io_(io),
      codec_(max_body_size) {}

Status CoroutineFrameChannel::ReadFrame(int fd, ProtocolFrame* frame) {
    if (io_ == nullptr || frame == nullptr) {
        return Status::Error(StatusCode::kServerError, "invalid coroutine frame channel");
    }

    while (true) {
        std::string error;
        const DecodeResult result = codec_.TryDecode(read_buffer_, frame, &error);
        if (result == DecodeResult::kSuccess) {
            return Status::Ok();
        }
        if (result == DecodeResult::kProtocolError) {
            return Status::Error(StatusCode::kProtocolError, error);
        }

        char chunk[kReadChunkSize];
        const ssize_t n = io_->Read(fd, chunk, sizeof(chunk));
        if (n > 0) {
            read_buffer_.append(chunk, static_cast<std::size_t>(n));
            continue;
        }
        if (n == 0) {
            return Status::Error(StatusCode::kNetworkError, "peer closed connection");
        }
        return Status::Error(StatusCode::kNetworkError, LastSocketError("coroutine read failed", errno));
    }
}

Status CoroutineFrameChannel::WriteFrame(int fd, const ProtocolFrame& frame) {
    if (io_ == nullptr) {
        return Status::Error(StatusCode::kServerError, "invalid coroutine frame channel");
    }
    const std::string data = codec_.Encode(frame);
    const ssize_t n = io_->WriteAll(fd, data.data(), data.size());
    if (n == static_cast<ssize_t>(data.size())) {
        return Status::Ok();
    }
    return Status::Error(StatusCode::kNetworkError, LastSocketError("coroutine write failed", errno));
}

std::size_t CoroutineFrameChannel::buffered_bytes() const noexcept {
    return read_buffer_.size();
}

}  // namespace minirpc
