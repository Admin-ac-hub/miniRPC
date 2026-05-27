#include "minirpc/observability/logger.h"

#include <iostream>

namespace minirpc {
namespace {

const char* ToString(LogLevel level) {
    switch (level) {
        case LogLevel::kDebug:
            return "DEBUG";
        case LogLevel::kInfo:
            return "INFO";
        case LogLevel::kWarn:
            return "WARN";
        case LogLevel::kError:
            return "ERROR";
    }
    return "UNKNOWN";
}

}  // namespace

void Logger::Log(LogLevel level, const std::string& message) {
    std::cerr << "[" << ToString(level) << "] " << message << '\n';
}

}  // namespace minirpc

