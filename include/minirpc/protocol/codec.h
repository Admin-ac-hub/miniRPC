#pragma once

#include <cstddef>
#include <string>

#include "minirpc/protocol/frame.h"

namespace minirpc {

enum class DecodeResult {
    kNeedMoreData,
    kSuccess,
    kProtocolError
};

class RpcCodec {
public:
    explicit RpcCodec(std::size_t max_body_size = kDefaultMaxFrameBodySize);

    std::string Encode(const ProtocolFrame& frame) const;
    DecodeResult TryDecode(std::string& buffer, ProtocolFrame* frame, std::string* error) const;

private:
    std::size_t max_body_size_;
};

}  // namespace minirpc

