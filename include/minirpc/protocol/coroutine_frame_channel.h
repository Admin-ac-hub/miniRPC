#pragma once

#include <cstddef>
#include <string>

#include "minirpc/core/status.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/runtime/coroutine_io_context.h"

namespace minirpc {

class CoroutineFrameChannel {
public:
    explicit CoroutineFrameChannel(CoroutineIoContext* io,
                                   std::size_t max_body_size = kDefaultMaxFrameBodySize);

    CoroutineFrameChannel(const CoroutineFrameChannel&) = delete;
    CoroutineFrameChannel& operator=(const CoroutineFrameChannel&) = delete;

    Status ReadFrame(int fd, ProtocolFrame* frame);
    Status WriteFrame(int fd, const ProtocolFrame& frame);

    std::size_t buffered_bytes() const noexcept;

private:
    CoroutineIoContext* io_;
    RpcCodec codec_;
    std::string read_buffer_;
};

}  // namespace minirpc
