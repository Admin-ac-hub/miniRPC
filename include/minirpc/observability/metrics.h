#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

namespace minirpc {

struct RpcMetricsSnapshot {
    uint64_t active_connections = 0;
    uint64_t total_requests = 0;
    uint64_t total_responses = 0;
    uint64_t success_requests = 0;
    uint64_t failed_requests = 0;
    uint64_t timeout_requests = 0;
    uint64_t rejected_requests = 0;
    uint64_t pending_requests = 0;
    uint64_t latency_samples = 0;
    uint64_t avg_latency_us = 0;
    uint64_t p50_latency_us = 0;
    uint64_t p95_latency_us = 0;
    uint64_t p99_latency_us = 0;
    uint64_t backpressure_connections = 0;
    uint64_t max_write_buffer_size = 0;
    uint64_t server_state = 2;
    uint64_t shutdown_start_time_ms = 0;
    uint64_t graceful_shutdown_timeout_count = 0;
    uint64_t protocol_error_total = 0;
};

class RpcMetrics {
public:
    void RecordRequest();
    void RecordResponse();
    void RecordSuccess();
    void RecordFailure();
    void RecordTimeout();
    void RecordRejected();
    void RecordProtocolError();
    void RecordLatency(std::chrono::nanoseconds latency);
    void IncrementActiveConnections();
    void DecrementActiveConnections();
    void IncrementPendingRequests();
    void DecrementPendingRequests();
    void EnterBackpressure();
    void LeaveBackpressure();
    void UpdateMaxWriteBufferSize(std::size_t size);
    void SetServerState(uint64_t state);
    void SetShutdownStartTimeMs(uint64_t unix_time_ms);
    void RecordGracefulShutdownTimeout();

    uint64_t active_connections() const;
    uint64_t total_requests() const;
    uint64_t total_responses() const;
    uint64_t success_requests() const;
    uint64_t failed_requests() const;
    uint64_t timeout_requests() const;
    uint64_t rejected_requests() const;
    uint64_t protocol_error_total() const;
    uint64_t pending_requests() const;
    uint64_t latency_samples() const;
    uint64_t avg_latency_us() const;
    uint64_t p50_latency_us() const;
    uint64_t p95_latency_us() const;
    uint64_t p99_latency_us() const;
    uint64_t backpressure_connections() const;
    uint64_t max_write_buffer_size() const;
    uint64_t server_state() const;
    uint64_t shutdown_start_time_ms() const;
    uint64_t graceful_shutdown_timeout_count() const;
    RpcMetricsSnapshot Snapshot() const;
    std::string ToPrometheusText(std::size_t threadpool_queue_size = 0) const;

private:
    static constexpr std::size_t kLatencySampleCapacity = 10000;
    static constexpr std::size_t kLatencyShardCount = 8;

    struct LatencyPercentiles {
        uint64_t p50_us = 0;
        uint64_t p95_us = 0;
        uint64_t p99_us = 0;
    };

    struct LatencySample {
        std::atomic<uint64_t> encoded_us{0};
    };

    struct alignas(64) LatencyShard {
        std::atomic<uint64_t> samples{0};
        std::atomic<uint64_t> ns{0};
    };

    LatencyPercentiles CalculateLatencyPercentilesUs() const;

    std::atomic<uint64_t> active_connections_{0};
    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> total_responses_{0};
    std::atomic<uint64_t> success_requests_{0};
    std::atomic<uint64_t> failed_requests_{0};
    std::atomic<uint64_t> timeout_requests_{0};
    std::atomic<uint64_t> rejected_requests_{0};
    std::atomic<uint64_t> protocol_error_total_{0};
    std::atomic<uint64_t> pending_requests_{0};
    std::array<LatencyShard, kLatencyShardCount> latency_shards_{};
    std::atomic<uint64_t> backpressure_connections_{0};
    std::atomic<uint64_t> max_write_buffer_size_{0};
    std::atomic<uint64_t> server_state_{2};
    std::atomic<uint64_t> shutdown_start_time_ms_{0};
    std::atomic<uint64_t> graceful_shutdown_timeout_count_{0};
    std::array<LatencySample, kLatencySampleCapacity> latency_ring_{};
};

}  // namespace minirpc
