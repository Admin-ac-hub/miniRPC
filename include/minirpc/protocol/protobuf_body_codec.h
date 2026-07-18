#pragma once

#include <cstdint>
#include <string>

#include "minirpc/protocol/message.h"

namespace minirpc {

std::string ProtobufEncodeRequestBody(const RpcRequest& request);
RpcRequest  ProtobufDecodeRequestBody(uint64_t request_id, const std::string& body);

std::string ProtobufEncodeResponseBody(const RpcResponse& response);
RpcResponse ProtobufDecodeResponseBody(uint64_t request_id, const std::string& body);

}  // namespace minirpc
