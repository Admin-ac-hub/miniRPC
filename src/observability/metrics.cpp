#include "minirpc/observability/metrics.h"

#include <algorithm>
#include <sstream>
#include <vector>

namespace minirpc {
namespace {

void SaturatingDecrement(std::atomic<uint64_t>& value) {
    uint64_t current = value.load(std::memory_order_relaxed);
    while (current > 0 &&
           !value.compare_exchange_weak(
               current, current - 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

uint64_t PercentileFromSortedSamples(const std::vector<uint64_t>& samples,
                                     double percentile) {
    if (samples.empty()) {
        return 0;
    }
    const double index = percentile * static_cast<double>(samples.size() - 1);
    return samples[static_cast<std::size_t>(index + 0.5)];
}

}  // namespace

void RpcMetrics::RecordRequest() {
    total_requests_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::RecordResponse() {
    total_responses_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::RecordSuccess() {
    success_requests_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::RecordFailure() {
    failed_requests_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::RecordTimeout() {
    timeout_requests_.fetch_add(1, std::memory_order_relaxed);
    RecordFailure();
}

void RpcMetrics::RecordRejected() {
    rejected_requests_.fetch_add(1, std::memory_order_relaxed);
    RecordFailure();
}

void RpcMetrics::RecordProtocolError() {
    protocol_error_total_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::RecordLatency(std::chrono::nanoseconds latency) {
    // Cache only a shard number so thread reuse cannot retain a destroyed metrics object.
    static std::atomic<std::size_t> next_shard{0};
    thread_local const std::size_t shard =
        next_shard.fetch_add(1, std::memory_order_relaxed) % kLatencyShardCount;
    const auto clamped = std::max<std::chrono::nanoseconds>(latency, std::chrono::nanoseconds::zero());
    const uint64_t index = latency_shards_[shard].samples.fetch_add(1, std::memory_order_relaxed);
    latency_shards_[shard].ns.fetch_add(static_cast<uint64_t>(clamped.count()),
                                       std::memory_order_relaxed);
    const uint64_t latency_us = static_cast<uint64_t>(clamped.count() / 1000);
    // Zero marks an unused slot; zero-duration samples are stored as one.
    // A coprime stride spreads writes while each shard still visits every slot.
    const std::size_t slot = ((index % latency_ring_.size()) * 17 +
                              shard * (kLatencySampleCapacity / kLatencyShardCount)) %
                             latency_ring_.size();
    latency_ring_[slot].encoded_us.store(
        latency_us + 1, std::memory_order_relaxed);
}

void RpcMetrics::IncrementActiveConnections() {
    active_connections_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::DecrementActiveConnections() {
    SaturatingDecrement(active_connections_);
}

void RpcMetrics::IncrementPendingRequests() {
    pending_requests_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::DecrementPendingRequests() {
    SaturatingDecrement(pending_requests_);
}

void RpcMetrics::EnterBackpressure() {
    backpressure_connections_.fetch_add(1, std::memory_order_relaxed);
}

void RpcMetrics::LeaveBackpressure() {
    SaturatingDecrement(backpressure_connections_);
}

void RpcMetrics::UpdateMaxWriteBufferSize(std::size_t size) {
    uint64_t current = max_write_buffer_size_.load(std::memory_order_relaxed);
    while (size > current &&
           !max_write_buffer_size_.compare_exchange_weak(
               current, static_cast<uint64_t>(size),
               std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

void RpcMetrics::SetServerState(uint64_t state) {
    server_state_.store(state, std::memory_order_relaxed);
}

void RpcMetrics::SetShutdownStartTimeMs(uint64_t unix_time_ms) {
    shutdown_start_time_ms_.store(unix_time_ms, std::memory_order_relaxed);
}

void RpcMetrics::RecordGracefulShutdownTimeout() {
    graceful_shutdown_timeout_count_.fetch_add(1, std::memory_order_relaxed);
}

uint64_t RpcMetrics::active_connections() const {
    return active_connections_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::total_requests() const {
    return total_requests_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::total_responses() const {
    return total_responses_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::success_requests() const {
    return success_requests_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::failed_requests() const {
    return failed_requests_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::timeout_requests() const {
    return timeout_requests_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::rejected_requests() const {
    return rejected_requests_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::protocol_error_total() const {
    return protocol_error_total_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::pending_requests() const {
    return pending_requests_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::latency_samples() const {
    uint64_t samples = 0;
    for (const auto& total : latency_shards_) {
        samples += total.samples.load(std::memory_order_relaxed);
    }
    return samples;
}

uint64_t RpcMetrics::avg_latency_us() const {
    const uint64_t samples = latency_samples();
    if (samples == 0) {
        return 0;
    }
    uint64_t total_ns = 0;
    for (const auto& total : latency_shards_) {
        total_ns += total.ns.load(std::memory_order_relaxed);
    }
    return total_ns / samples / 1000;
}

uint64_t RpcMetrics::p50_latency_us() const {
    return CalculateLatencyPercentilesUs().p50_us;
}

uint64_t RpcMetrics::p95_latency_us() const {
    return CalculateLatencyPercentilesUs().p95_us;
}

uint64_t RpcMetrics::p99_latency_us() const {
    return CalculateLatencyPercentilesUs().p99_us;
}

uint64_t RpcMetrics::backpressure_connections() const {
    return backpressure_connections_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::max_write_buffer_size() const {
    return max_write_buffer_size_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::server_state() const {
    return server_state_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::shutdown_start_time_ms() const {
    return shutdown_start_time_ms_.load(std::memory_order_relaxed);
}

uint64_t RpcMetrics::graceful_shutdown_timeout_count() const {
    return graceful_shutdown_timeout_count_.load(std::memory_order_relaxed);
}

RpcMetricsSnapshot RpcMetrics::Snapshot() const {
    RpcMetricsSnapshot snapshot;
    snapshot.active_connections = active_connections();
    snapshot.total_requests = total_requests();
    snapshot.total_responses = total_responses();
    snapshot.success_requests = success_requests();
    snapshot.failed_requests = failed_requests();
    snapshot.timeout_requests = timeout_requests();
    snapshot.rejected_requests = rejected_requests();
    snapshot.protocol_error_total = protocol_error_total();
    snapshot.pending_requests = pending_requests();
    snapshot.latency_samples = latency_samples();
    snapshot.avg_latency_us = avg_latency_us();
    const LatencyPercentiles latency_percentiles = CalculateLatencyPercentilesUs();
    snapshot.p50_latency_us = latency_percentiles.p50_us;
    snapshot.p95_latency_us = latency_percentiles.p95_us;
    snapshot.p99_latency_us = latency_percentiles.p99_us;
    snapshot.backpressure_connections = backpressure_connections();
    snapshot.max_write_buffer_size = max_write_buffer_size();
    snapshot.server_state = server_state();
    snapshot.shutdown_start_time_ms = shutdown_start_time_ms();
    snapshot.graceful_shutdown_timeout_count = graceful_shutdown_timeout_count();
    return snapshot;
}

std::string RpcMetrics::ToPrometheusText(std::size_t threadpool_queue_size) const {
    const RpcMetricsSnapshot snapshot = Snapshot();
    std::ostringstream out;
    out << "# TYPE minirpc_active_connections gauge\n";
    out << "minirpc_active_connections " << snapshot.active_connections << '\n';
    out << "# TYPE minirpc_requests_total counter\n";
    out << "minirpc_requests_total " << snapshot.total_requests << '\n';
    out << "# TYPE minirpc_responses_total counter\n";
    out << "minirpc_responses_total " << snapshot.total_responses << '\n';
    out << "# TYPE minirpc_success_requests_total counter\n";
    out << "minirpc_success_requests_total " << snapshot.success_requests << '\n';
    out << "# TYPE minirpc_failed_requests_total counter\n";
    out << "minirpc_failed_requests_total " << snapshot.failed_requests << '\n';
    out << "# TYPE minirpc_timeout_requests_total counter\n";
    out << "minirpc_timeout_requests_total " << snapshot.timeout_requests << '\n';
    out << "# TYPE minirpc_rejected_requests_total counter\n";
    out << "minirpc_rejected_requests_total " << snapshot.rejected_requests << '\n';
    out << "# TYPE minirpc_protocol_error_total counter\n";
    out << "minirpc_protocol_error_total " << snapshot.protocol_error_total << '\n';
    out << "# TYPE minirpc_pending_requests gauge\n";
    out << "minirpc_pending_requests " << snapshot.pending_requests << '\n';
    out << "# TYPE minirpc_latency_samples_total counter\n";
    out << "minirpc_latency_samples_total " << snapshot.latency_samples << '\n';
    out << "# TYPE minirpc_avg_latency_us gauge\n";
    out << "minirpc_avg_latency_us " << snapshot.avg_latency_us << '\n';
    out << "# TYPE minirpc_p50_latency_us gauge\n";
    out << "minirpc_p50_latency_us " << snapshot.p50_latency_us << '\n';
    out << "# TYPE minirpc_p95_latency_us gauge\n";
    out << "minirpc_p95_latency_us " << snapshot.p95_latency_us << '\n';
    out << "# TYPE minirpc_p99_latency_us gauge\n";
    out << "minirpc_p99_latency_us " << snapshot.p99_latency_us << '\n';
    out << "# TYPE minirpc_backpressure_connections gauge\n";
    out << "minirpc_backpressure_connections " << snapshot.backpressure_connections << '\n';
    out << "# TYPE minirpc_max_write_buffer_size gauge\n";
    out << "minirpc_max_write_buffer_size " << snapshot.max_write_buffer_size << '\n';
    out << "# TYPE minirpc_server_state gauge\n";
    out << "minirpc_server_state " << snapshot.server_state << '\n';
    out << "# TYPE minirpc_inflight_requests gauge\n";
    out << "minirpc_inflight_requests " << snapshot.pending_requests << '\n';
    out << "# TYPE minirpc_shutdown_start_time_ms gauge\n";
    out << "minirpc_shutdown_start_time_ms " << snapshot.shutdown_start_time_ms << '\n';
    out << "# TYPE minirpc_graceful_shutdown_timeout_total counter\n";
    out << "minirpc_graceful_shutdown_timeout_total "
        << snapshot.graceful_shutdown_timeout_count << '\n';
    out << "# TYPE minirpc_threadpool_queue_size gauge\n";
    out << "minirpc_threadpool_queue_size " << threadpool_queue_size << '\n';
    return out.str();
}

RpcMetrics::LatencyPercentiles RpcMetrics::CalculateLatencyPercentilesUs() const {
    std::vector<uint64_t> samples;
    samples.reserve(latency_ring_.size());
    // Slots are independent: concurrent writes produce a best-effort recent window.
    for (const auto& slot : latency_ring_) {
        const uint64_t encoded = slot.encoded_us.load(std::memory_order_relaxed);
        if (encoded != 0) samples.push_back(encoded - 1);
    }
    std::sort(samples.begin(), samples.end());

    LatencyPercentiles percentiles;
    percentiles.p50_us = PercentileFromSortedSamples(samples, 0.50);
    percentiles.p95_us = PercentileFromSortedSamples(samples, 0.95);
    percentiles.p99_us = PercentileFromSortedSamples(samples, 0.99);
    return percentiles;
}

}  // namespace minirpc
