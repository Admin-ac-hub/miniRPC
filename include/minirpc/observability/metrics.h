#pragma once

#include <atomic>
#include <cstdint>

namespace minirpc {

class RpcMetrics {
public:
    void RecordRequest();
    void RecordSuccess();
    void RecordFailure();

    uint64_t total_requests() const;
    uint64_t success_requests() const;
    uint64_t failed_requests() const;

private:
    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> success_requests_{0};
    std::atomic<uint64_t> failed_requests_{0};
};

}  // namespace minirpc

