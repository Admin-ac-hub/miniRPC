#pragma once

#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

#include "minirpc/protocol/message.h"

namespace minirpc {

using RpcHandler = std::function<RpcResponse(const RpcRequest&)>;

class ServiceRegistry {
public:
    void Register(const std::string& service_name,
                  const std::string& method_name,
                  RpcHandler handler);

    RpcHandler Find(const std::string& service_name,
                    const std::string& method_name) const;

    bool HasService(const std::string& service_name) const;

private:
    using MethodMap = std::unordered_map<std::string, RpcHandler>;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, MethodMap> services_;
};

}  // namespace minirpc

