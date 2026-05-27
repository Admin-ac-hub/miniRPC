#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "minirpc/core/endpoint.h"

namespace minirpc {

class ServiceResolver {
public:
    void AddEndpoint(const std::string& service_name, Endpoint endpoint);
    std::vector<Endpoint> Resolve(const std::string& service_name) const;

private:
    std::unordered_map<std::string, std::vector<Endpoint>> endpoints_;
};

}  // namespace minirpc

