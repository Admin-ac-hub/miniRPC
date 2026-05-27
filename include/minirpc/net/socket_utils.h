#pragma once

#include <cstddef>
#include <string>

namespace minirpc {

enum class SendStatus {
    kOk,
    kWouldBlock,
    kIoError
};

struct SendResult {
    SendStatus status = SendStatus::kOk;
    std::size_t sent = 0;
    int error_code = 0;
};

bool SetNonBlocking(int fd, std::string* error);
SendResult SendAll(int fd, const char* data, std::size_t size);
std::string LastSocketError(const std::string& action, int error_code);

}  // namespace minirpc

