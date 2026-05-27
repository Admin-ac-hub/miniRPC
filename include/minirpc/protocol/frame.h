#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "minirpc/protocol/message.h"

namespace minirpc {

constexpr uint32_t kProtocolMagic = 0x4d525043;
constexpr uint16_t kProtocolVersion = 1;
constexpr std::size_t kProtocolHeaderSize = 20;
constexpr std::size_t kDefaultMaxFrameBodySize = 16 * 1024 * 1024;

struct ProtocolFrame {
    uint16_t version = kProtocolVersion;
    MessageType message_type = MessageType::kRequest;
    CodecType codec_type = CodecType::kRaw;
    uint64_t request_id = 0;
    std::string body;
};

}  // namespace minirpc

