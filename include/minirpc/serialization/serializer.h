#pragma once

#include <string>

#include "minirpc/protocol/message.h"

namespace minirpc {

class Serializer {
public:
    virtual ~Serializer() = default;

    virtual CodecType codec_type() const = 0;
    virtual std::string SerializeRequest(const RpcRequest& request) const = 0;
    virtual RpcRequest DeserializeRequest(const std::string& data) const = 0;
    virtual std::string SerializeResponse(const RpcResponse& response) const = 0;
    virtual RpcResponse DeserializeResponse(const std::string& data) const = 0;
};

}  // namespace minirpc

