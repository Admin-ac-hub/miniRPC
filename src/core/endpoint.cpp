#include "minirpc/core/endpoint.h"

namespace minirpc {

std::string Endpoint::ToString() const {
    return host + ":" + std::to_string(port);
}

bool Endpoint::operator==(const Endpoint& other) const {
    return host == other.host && port == other.port;
}

}  // namespace minirpc

