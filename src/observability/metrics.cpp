#include "minirpc/observability/metrics.h"

namespace minirpc {

void RpcMetrics::RecordRequest() {
    total_requests_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::RecordSuccess() {
    success_requests_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::RecordFailure() {
    failed_requests_.fetch_add(1, std::memory_order_relaxed);
}

uint64_t RpcMetrics::total_requests() const {
    return total_requests_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::success_requests() const {
    return success_requests_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::failed_requests() const {
    return failed_requests_.load(std::memory_order_relaxed);
}

}  // namespace minirpc

