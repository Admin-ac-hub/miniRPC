#include "minirpc/discovery/service_resolver.h"

#include <utility>

namespace minirpc {

void ServiceResolver::AddEndpoint(const std::string& service_name, Endpoint endpoint) {
    endpoints_[service_name].push_back(std::move(endpoint));
}

std::vector<Endpoint> ServiceResolver::Resolve(const std::string& service_name) const {
    auto it = endpoints_.find(service_name);
    if (it == endpoints_.end()) {
        return {};
    }
    return it->second;
}

}  // namespace minirpc

