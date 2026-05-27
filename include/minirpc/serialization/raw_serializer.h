#pragma once

#include "minirpc/serialization/serializer.h"

namespace minirpc {

class RawSerializer final : public Serializer {
public:
    CodecType codec_type() const override;
    std::string SerializeRequest(const RpcRequest& request) const override;
    RpcRequest DeserializeRequest(const std::string& data) const override;
    std::string SerializeResponse(const RpcResponse& response) const override;
    RpcResponse DeserializeResponse(const std::string& data) const override;
};

}  // namespace minirpc

