#pragma once

#include <string>

namespace minirpc {

enum class StatusCode {
    kOk = 0,
    kTimeout = 1,
    kNetworkError = 2,
    kServiceNotFound = 3,
    kMethodNotFound = 4,
    kSerializeError = 5,
    kDeserializeError = 6,
    kProtocolError = 7,
    kServerError = 8,
    kNotImplemented = 9
};

class Status {
public:
    Status();
    Status(StatusCode code, std::string message);

    static Status Ok();
    static Status Error(StatusCode code, std::string message);

    bool ok() const;
    StatusCode code() const;
    const std::string& message() const;
    std::string ToString() const;

private:
    StatusCode code_;
    std::string message_;
};

}  // namespace minirpc

