#include "minirpc/discovery/load_balancer.h"

#include <stdexcept>

namespace minirpc {

Endpoint RoundRobinLoadBalancer::Select(const std::string& service_name,
                                        const std::vector<Endpoint>& endpoints) {
    if (endpoints.empty()) {
        throw std::invalid_argument("empty endpoint list");
    }

    std::size_t& index = next_index_[service_name];
    Endpoint selected = endpoints[index % endpoints.size()];
    ++index;
    return selected;
}

}  // namespace minirpc

