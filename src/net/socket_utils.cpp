#include "minirpc/net/socket_utils.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/socket.h>

namespace minirpc {

std::string LastSocketError(const std::string& action, int error_code) {
    return action + ": " + std::strerror(error_code);
}

bool SetNonBlocking(int fd, std::string* error) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        if (error != nullptr) {
            *error = LastSocketError("fcntl(F_GETFL) failed", errno);
        }
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        if (error != nullptr) {
            *error = LastSocketError("fcntl(F_SETFL) failed", errno);
        }
        return false;
    }
    return true;
}

SendResult SendAll(int fd, const char* data, std::size_t size) {
    SendResult result;
    while (result.sent < size) {
        const ssize_t n = ::send(fd, data + result.sent, size - result.sent, MSG_NOSIGNAL);
        if (n > 0) {
            result.sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            result.status = SendStatus::kWouldBlock;
            return result;
        }
        result.status = SendStatus::kIoError;
        result.error_code = errno;
        return result;
    }
    result.status = SendStatus::kOk;
    return result;
}

}  // namespace minirpc
