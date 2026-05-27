#pragma once

#include <string>

namespace minirpc {

enum class LogLevel {
    kDebug,
    kInfo,
    kWarn,
    kError
};

class Logger {
public:
    static void Log(LogLevel level, const std::string& message);
};

}  // namespace minirpc

