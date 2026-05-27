#include "minirpc/serialization/raw_serializer.h"

namespace minirpc {

CodecType RawSerializer::codec_type() const {
    return CodecType::kRaw;
}

std::string RawSerializer::SerializeRequest(const RpcRequest& request) const {
    return request.payload;
}

RpcRequest RawSerializer::DeserializeRequest(const std::string& data) const {
    RpcRequest request;
    request.payload = data;
    return request;
}

std::string RawSerializer::SerializeResponse(const RpcResponse& response) const {
    return response.payload;
}

RpcResponse RawSerializer::DeserializeResponse(const std::string& data) const {
    RpcResponse response;
    response.payload = data;
    return response;
}

}  // namespace minirpc

