#include "minirpc/protocol/body_codec.h"

#include "minirpc/protocol/protobuf_body_codec.h"

namespace minirpc {

BodyCodecError::BodyCodecError(std::string message)
    : message_(std::move(message)) {}

const char* BodyCodecError::what() const noexcept { return message_.c_str(); }

std::string EncodeRequestBody(const RpcRequest& request) {
    return ProtobufEncodeRequestBody(request);
}

RpcRequest DecodeRequestBody(uint64_t request_id, const std::string& body) {
    return ProtobufDecodeRequestBody(request_id, body);
}

std::string EncodeResponseBody(const RpcResponse& response) {
    return ProtobufEncodeResponseBody(response);
}

RpcResponse DecodeResponseBody(uint64_t request_id, const std::string& body) {
    return ProtobufDecodeResponseBody(request_id, body);
}

}  // namespace minirpc
