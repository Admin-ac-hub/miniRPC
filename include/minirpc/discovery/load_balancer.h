#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "minirpc/core/endpoint.h"

namespace minirpc {

class RoundRobinLoadBalancer {
public:
    Endpoint Select(const std::string& service_name, const std::vector<Endpoint>& endpoints);

private:
    std::unordered_map<std::string, std::size_t> next_index_;
};

}  // namespace minirpc

