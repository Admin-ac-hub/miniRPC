#include <cassert>
#include <chrono>
#include <string>

#include "minirpc/observability/metrics.h"

namespace {

bool ContainsLine(const std::string& text, const std::string& line) {
    return text.find(line + "\n") != std::string::npos;
}

void TestFailedResponseCount() {
    minirpc::RpcMetrics metrics;
    metrics.RecordRequest();
    metrics.RecordResponse();
    metrics.RecordFailure();

    const minirpc::RpcMetricsSnapshot snapshot = metrics.Snapshot();
    assert(snapshot.total_requests == 1);
    assert(snapshot.total_responses == 1);
    assert(snapshot.success_requests == 0);
    assert(snapshot.failed_requests == 1);
    assert(ContainsLine(metrics.ToPrometheusText(), "minirpc_responses_total 1"));
}

void TestSnapshotPercentilesMatchPublicAccessors() {
    minirpc::RpcMetrics metrics;
    metrics.RecordLatency(std::chrono::microseconds(40));
    metrics.RecordLatency(std::chrono::microseconds(10));
    metrics.RecordLatency(std::chrono::microseconds(30));
    metrics.RecordLatency(std::chrono::microseconds(20));

    const minirpc::RpcMetricsSnapshot snapshot = metrics.Snapshot();
    assert(snapshot.p50_latency_us == 30);
    assert(snapshot.p95_latency_us == 40);
    assert(snapshot.p99_latency_us == 40);
    assert(snapshot.p50_latency_us == metrics.p50_latency_us());
    assert(snapshot.p95_latency_us == metrics.p95_latency_us());
    assert(snapshot.p99_latency_us == metrics.p99_latency_us());
}

}  // namespace

int main() {
    TestFailedResponseCount();
    TestSnapshotPercentilesMatchPublicAccessors();

    minirpc::RpcMetrics metrics;

    assert(metrics.active_connections() == 0);
    assert(metrics.total_requests() == 0);
    assert(metrics.total_responses() == 0);
    assert(metrics.success_requests() == 0);
    assert(metrics.failed_requests() == 0);
    assert(metrics.timeout_requests() == 0);
    assert(metrics.rejected_requests() == 0);
    assert(metrics.pending_requests() == 0);
    assert(metrics.latency_samples() == 0);
    assert(metrics.avg_latency_us() == 0);
    assert(metrics.p50_latency_us() == 0);
    assert(metrics.p95_latency_us() == 0);
    assert(metrics.p99_latency_us() == 0);
    assert(metrics.backpressure_connections() == 0);
    assert(metrics.max_write_buffer_size() == 0);
    assert(metrics.server_state() == 2);
    assert(metrics.shutdown_start_time_ms() == 0);
    assert(metrics.graceful_shutdown_timeout_count() == 0);

    metrics.IncrementActiveConnections();
    metrics.IncrementActiveConnections();
    metrics.DecrementActiveConnections();
    assert(metrics.active_connections() == 1);

    metrics.RecordRequest();
    metrics.RecordResponse();
    metrics.RecordSuccess();
    metrics.IncrementPendingRequests();
    metrics.DecrementPendingRequests();
    assert(metrics.total_requests() == 1);
    assert(metrics.total_responses() == 1);
    assert(metrics.success_requests() == 1);
    assert(metrics.pending_requests() == 0);

    metrics.RecordRequest();
    metrics.RecordResponse();
    metrics.RecordRejected();
    metrics.RecordRequest();
    metrics.RecordResponse();
    metrics.RecordTimeout();
    assert(metrics.total_requests() == 3);
    assert(metrics.total_responses() == 3);
    assert(metrics.failed_requests() == 2);
    assert(metrics.rejected_requests() == 1);
    assert(metrics.timeout_requests() == 1);

    metrics.RecordLatency(std::chrono::microseconds(10));
    metrics.RecordLatency(std::chrono::microseconds(20));
    metrics.RecordLatency(std::chrono::microseconds(2000));
    assert(metrics.latency_samples() == 3);
    assert(metrics.avg_latency_us() >= 600);
    assert(metrics.p50_latency_us() >= 16);
    assert(metrics.p95_latency_us() >= 2000);
    assert(metrics.p99_latency_us() >= 2000);

    metrics.EnterBackpressure();
    metrics.UpdateMaxWriteBufferSize(4096);
    metrics.UpdateMaxWriteBufferSize(1024);
    metrics.SetServerState(1);
    metrics.SetShutdownStartTimeMs(123456);
    metrics.RecordGracefulShutdownTimeout();
    assert(metrics.backpressure_connections() == 1);
    assert(metrics.max_write_buffer_size() == 4096);
    assert(metrics.server_state() == 1);
    assert(metrics.shutdown_start_time_ms() == 123456);
    assert(metrics.graceful_shutdown_timeout_count() == 1);

    const minirpc::RpcMetricsSnapshot snapshot = metrics.Snapshot();
    assert(snapshot.active_connections == 1);
    assert(snapshot.total_requests == 3);
    assert(snapshot.total_responses == 3);
    assert(snapshot.success_requests == 1);
    assert(snapshot.failed_requests == 2);
    assert(snapshot.timeout_requests == 1);
    assert(snapshot.rejected_requests == 1);
    assert(snapshot.pending_requests == 0);
    assert(snapshot.latency_samples == 3);
    assert(snapshot.p50_latency_us >= 16);
    assert(snapshot.p95_latency_us >= 2000);
    assert(snapshot.backpressure_connections == 1);
    assert(snapshot.max_write_buffer_size == 4096);
    assert(snapshot.server_state == 1);
    assert(snapshot.shutdown_start_time_ms == 123456);
    assert(snapshot.graceful_shutdown_timeout_count == 1);

    const std::string text = metrics.ToPrometheusText(7);
    assert(ContainsLine(text, "# TYPE minirpc_active_connections gauge"));
    assert(ContainsLine(text, "minirpc_active_connections 1"));
    assert(ContainsLine(text, "minirpc_requests_total 3"));
    assert(ContainsLine(text, "minirpc_responses_total 3"));
    assert(ContainsLine(text, "minirpc_success_requests_total 1"));
    assert(ContainsLine(text, "minirpc_failed_requests_total 2"));
    assert(ContainsLine(text, "minirpc_timeout_requests_total 1"));
    assert(ContainsLine(text, "minirpc_rejected_requests_total 1"));
    assert(ContainsLine(text, "minirpc_pending_requests 0"));
    assert(ContainsLine(text, "minirpc_latency_samples_total 3"));
    assert(ContainsLine(text, "minirpc_threadpool_queue_size 7"));
    assert(ContainsLine(text, "minirpc_backpressure_connections 1"));
    assert(ContainsLine(text, "minirpc_max_write_buffer_size 4096"));
    assert(ContainsLine(text, "minirpc_server_state 1"));
    assert(ContainsLine(text, "minirpc_inflight_requests 0"));
    assert(ContainsLine(text, "minirpc_shutdown_start_time_ms 123456"));
    assert(ContainsLine(text, "minirpc_graceful_shutdown_timeout_total 1"));

    metrics.DecrementActiveConnections();
    metrics.DecrementActiveConnections();
    metrics.LeaveBackpressure();
    metrics.LeaveBackpressure();
    assert(metrics.active_connections() == 0);
    assert(metrics.backpressure_connections() == 0);

    return 0;
}
