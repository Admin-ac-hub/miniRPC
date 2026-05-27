#pragma once

#include <exception>
#include <string>

#include "minirpc/protocol/message.h"

namespace minirpc {

class BodyCodecError final : public std::exception {
public:
    explicit BodyCodecError(std::string message);

    const char* what() const noexcept override;

private:
    std::string message_;
};

std::string EncodeRequestBody(const RpcRequest& request);
RpcRequest DecodeRequestBody(uint64_t request_id, const std::string& body);

std::string EncodeResponseBody(const RpcResponse& response);
RpcResponse DecodeResponseBody(uint64_t request_id, const std::string& body);

}  // namespace minirpc
