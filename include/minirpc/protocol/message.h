#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace minirpc {

enum class MessageType : uint8_t {
    kRequest = 1,
    kResponse = 2,
    kHeartbeatRequest = 3,
    kHeartbeatResponse = 4
};

enum class CodecType : uint8_t {
    kRaw = 0,
    kJson = 1,
    kProtobuf = 2
};

struct RpcRequest {
    uint64_t request_id = 0;
    std::string service_name;
    std::string method_name;
    std::string payload;
    int64_t deadline_unix_ms = 0;
};

struct RpcResponse {
    uint64_t request_id = 0;
    int32_t status_code = 0;
    std::string error_message;
    std::string payload;
};

}  // namespace minirpc
