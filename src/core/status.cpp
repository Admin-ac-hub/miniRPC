#include "minirpc/core/status.h"

#include <sstream>

namespace minirpc {

Status::Status() : code_(StatusCode::kOk), message_("OK") {}

Status::Status(StatusCode code, std::string message)
    : code_(code), message_(std::move(message)) {}

Status Status::Ok() {
    return Status();
}

Status Status::Error(StatusCode code, std::string message) {
    return Status(code, std::move(message));
}

bool Status::ok() const {
    return code_ == StatusCode::kOk;
}

StatusCode Status::code() const {
    return code_;
}

const std::string& Status::message() const {
    return message_;
}

std::string Status::ToString() const {
    std::ostringstream oss;
    oss << static_cast<int>(code_) << ": " << message_;
    return oss.str();
}

}  // namespace minirpc

