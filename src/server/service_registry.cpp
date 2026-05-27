#include "minirpc/server/service_registry.h"

#include <utility>

namespace minirpc {

void ServiceRegistry::Register(const std::string& service_name,
                               const std::string& method_name,
                               RpcHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    services_[service_name][method_name] = std::move(handler);
}

RpcHandler ServiceRegistry::Find(const std::string& service_name,
                                 const std::string& method_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto service_it = services_.find(service_name);
    if (service_it == services_.end()) {
        return nullptr;
    }
    auto method_it = service_it->second.find(method_name);
    if (method_it == service_it->second.end()) {
        return nullptr;
    }
    return method_it->second;
}

bool ServiceRegistry::HasService(const std::string& service_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return services_.find(service_name) != services_.end();
}

}  // namespace minirpc

