#include "minirpc/protocol/codec.h"

#include <limits>

namespace minirpc {
namespace {

void AppendUint16(std::string* output, uint16_t value) {
    output->push_back(static_cast<char>((value >> 8) & 0xff));
    output->push_back(static_cast<char>(value & 0xff));
}

void AppendUint32(std::string* output, uint32_t value) {
    output->push_back(static_cast<char>((value >> 24) & 0xff));
    output->push_back(static_cast<char>((value >> 16) & 0xff));
    output->push_back(static_cast<char>((value >> 8) & 0xff));
    output->push_back(static_cast<char>(value & 0xff));
}

void AppendUint64(std::string* output, uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        output->push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

uint16_t ReadUint16(const char* data) {
    return (static_cast<uint16_t>(static_cast<unsigned char>(data[0])) << 8) |
           static_cast<uint16_t>(static_cast<unsigned char>(data[1]));
}

uint32_t ReadUint32(const char* data) {
    return (static_cast<uint32_t>(static_cast<unsigned char>(data[0])) << 24) |
           (static_cast<uint32_t>(static_cast<unsigned char>(data[1])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(data[2])) << 8) |
           static_cast<uint32_t>(static_cast<unsigned char>(data[3]));
}

uint64_t ReadUint64(const char* data) {
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8) | static_cast<unsigned char>(data[i]);
    }
    return value;
}

bool IsKnownMessageType(uint8_t value) {
    return value >= static_cast<uint8_t>(MessageType::kRequest) &&
           value <= static_cast<uint8_t>(MessageType::kHeartbeatResponse);
}

bool IsKnownCodecType(uint8_t value) {
    return value <= static_cast<uint8_t>(CodecType::kProtobuf);
}

}  // namespace

RpcCodec::RpcCodec(std::size_t max_body_size) : max_body_size_(max_body_size) {}

std::string RpcCodec::Encode(const ProtocolFrame& frame) const {
    std::string output;
    output.reserve(kProtocolHeaderSize + frame.body.size());

    AppendUint32(&output, kProtocolMagic);
    AppendUint16(&output, frame.version);
    output.push_back(static_cast<char>(frame.message_type));
    output.push_back(static_cast<char>(frame.codec_type));
    AppendUint64(&output, frame.request_id);
    AppendUint32(&output, static_cast<uint32_t>(frame.body.size()));
    output.append(frame.body);

    return output;
}

DecodeResult RpcCodec::TryDecode(std::string& buffer,
                                 ProtocolFrame* frame,
                                 std::string* error) const {
    if (buffer.size() < kProtocolHeaderSize) {
        return DecodeResult::kNeedMoreData;
    }

    const char* data = buffer.data();
    const uint32_t magic = ReadUint32(data);
    if (magic != kProtocolMagic) {
        if (error != nullptr) {
            *error = "invalid protocol magic";
        }
        return DecodeResult::kProtocolError;
    }

    const uint16_t version = ReadUint16(data + 4);
    if (version != kProtocolVersion) {
        if (error != nullptr) {
            *error = "unsupported protocol version";
        }
        return DecodeResult::kProtocolError;
    }

    const uint8_t message_type = static_cast<unsigned char>(data[6]);
    if (!IsKnownMessageType(message_type)) {
        if (error != nullptr) {
            *error = "unsupported message type";
        }
        return DecodeResult::kProtocolError;
    }

    const uint8_t codec_type = static_cast<unsigned char>(data[7]);
    if (!IsKnownCodecType(codec_type)) {
        if (error != nullptr) {
            *error = "unsupported codec type";
        }
        return DecodeResult::kProtocolError;
    }

    const uint64_t request_id = ReadUint64(data + 8);
    const uint32_t body_size = ReadUint32(data + 16);
    if (body_size > max_body_size_) {
        if (error != nullptr) {
            *error = "frame body too large";
        }
        return DecodeResult::kProtocolError;
    }

    const std::size_t frame_size = kProtocolHeaderSize + static_cast<std::size_t>(body_size);
    if (buffer.size() < frame_size) {
        return DecodeResult::kNeedMoreData;
    }

    if (frame != nullptr) {
        frame->version = version;
        frame->message_type = static_cast<MessageType>(message_type);
        frame->codec_type = static_cast<CodecType>(codec_type);
        frame->request_id = request_id;
        frame->body.assign(buffer.data() + kProtocolHeaderSize, body_size);
    }

    buffer.erase(0, frame_size);
    return DecodeResult::kSuccess;
}

}  // namespace minirpc

