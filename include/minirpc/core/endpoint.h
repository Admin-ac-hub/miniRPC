#pragma once

#include <cstdint>
#include <string>

namespace minirpc {

struct Endpoint {
    std::string host;
    uint16_t port = 0;

    std::string ToString() const;
    bool operator==(const Endpoint& other) const;
};

}  // namespace minirpc

