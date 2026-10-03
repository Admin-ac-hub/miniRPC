#include <cassert>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/observability/metrics.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/server/rpc_server.h"

namespace {

bool ContainsLine(const std::string& text, const std::string& line) {
    return text.find(line + "\n") != std::string::npos;
}

void TestProtocolErrorDisconnectMetrics() {
    constexpr uint16_t kPort = 19610;
    minirpc::RpcServer server;
    assert(server.Start({"127.0.0.1", kPort}).ok());
    minirpc::RpcCodec codec;
    const std::string header = codec.Encode(minirpc::ProtocolFrame{});
    std::vector<std::string> invalid_headers(5, header);
    invalid_headers[0][0] = 0;
    invalid_headers[1][5] = 2;
    invalid_headers[2][6] = static_cast<char>(0xff);
    invalid_headers[3][7] = static_cast<char>(0xff);
    // 2 MiB body exceeds the default budget by the fixed header size.
    invalid_headers[4][17] = 0x20;

    for (std::size_t i = 0; i < invalid_headers.size(); ++i) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        assert(fd != -1);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(kPort);
        assert(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
        assert(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        timeval timeout{1, 0};
        assert(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
        const auto& bytes = invalid_headers[i];
        std::size_t sent = 0;
        while (sent < bytes.size()) {
            const ssize_t n = ::send(fd, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
            if (n < 0 && errno == EINTR) continue;
            assert(n > 0);
            sent += static_cast<std::size_t>(n);
        }
        char byte;
        ssize_t received;
        do {
            received = ::recv(fd, &byte, 1, 0);
        } while (received < 0 && errno == EINTR);
        assert(received == 0 || (received < 0 && errno == ECONNRESET));
        ::close(fd);
        assert(ContainsLine(server.MetricsText(),
                            "minirpc_protocol_error_total " + std::to_string(i + 1)));
    }
    server.Stop();
    const auto snapshot = server.metrics().Snapshot();
    assert(snapshot.protocol_error_total == invalid_headers.size());
    assert(server.metrics().protocol_error_total() == invalid_headers.size());
    assert(snapshot.total_requests == 0);
    assert(snapshot.rejected_requests == 0);
    assert(snapshot.failed_requests == 0);
    assert(snapshot.active_connections == 0);
    assert(ContainsLine(server.MetricsText(), "# TYPE minirpc_protocol_error_total counter"));
    assert(ContainsLine(server.MetricsText(), "minirpc_protocol_error_total 5"));
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

void TestLatencyWindowWrapAndZeroSamples() {
    minirpc::RpcMetrics metrics;
    for (int i = 0; i < 1000; ++i) metrics.RecordLatency(std::chrono::microseconds(1000));
    for (int i = 0; i < 10000; ++i) metrics.RecordLatency(std::chrono::microseconds(10));
    auto snapshot = metrics.Snapshot();
    assert(snapshot.latency_samples == 11000);
    assert(snapshot.avg_latency_us == 100);
    assert(snapshot.p50_latency_us == 10);
    assert(snapshot.p95_latency_us == 10);
    assert(snapshot.p99_latency_us == 10);
    for (int i = 0; i < 10000; ++i) metrics.RecordLatency(std::chrono::nanoseconds(-1));
    snapshot = metrics.Snapshot();
    assert(snapshot.latency_samples == 21000);
    assert(snapshot.p50_latency_us == 0);
    assert(snapshot.p95_latency_us == 0);
    assert(snapshot.p99_latency_us == 0);

    minirpc::RpcMetrics large;
    large.RecordLatency(std::chrono::nanoseconds::max());
    const auto max_us = static_cast<uint64_t>(std::chrono::nanoseconds::max().count() / 1000);
    assert(large.p50_latency_us() == max_us);
    assert(large.avg_latency_us() == max_us);
}

void TestConcurrentLatencyWritersAndSnapshots(int samples_per_writer) {
    minirpc::RpcMetrics metrics;
    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::thread reader([&] {
        while (!start.load()) std::this_thread::yield();
        do {
            const auto snapshot = metrics.Snapshot();
            assert(snapshot.p50_latency_us <= snapshot.p95_latency_us);
            assert(snapshot.p95_latency_us <= snapshot.p99_latency_us);
            assert(snapshot.p99_latency_us <= 40);
        } while (!done.load());
    });
    std::vector<std::thread> writers;
    for (int i = 1; i <= 4; ++i) {
        writers.emplace_back([&, i] {
            while (!start.load()) std::this_thread::yield();
            for (int j = 0; j < samples_per_writer; ++j) {
                metrics.RecordLatency(std::chrono::microseconds(
                    samples_per_writer > 2500 ? 40 : i * 10));
            }
        });
    }
    start.store(true);
    for (auto& writer : writers) writer.join();
    done.store(true);
    reader.join();
    const auto snapshot = metrics.Snapshot();
    assert(snapshot.latency_samples == static_cast<uint64_t>(4 * samples_per_writer));
    assert(snapshot.avg_latency_us == (samples_per_writer > 2500 ? 40 : 25));
    assert(snapshot.p50_latency_us == (samples_per_writer > 2500 ? 40 : 30));
    assert(snapshot.p95_latency_us == 40);
    assert(snapshot.p99_latency_us == 40);
}

}  // namespace

int main() {
    TestProtocolErrorDisconnectMetrics();
    TestFailedResponseCount();
    TestSnapshotPercentilesMatchPublicAccessors();
    TestLatencyWindowWrapAndZeroSamples();
    TestConcurrentLatencyWritersAndSnapshots(1000);
    TestConcurrentLatencyWritersAndSnapshots(20000);

    minirpc::RpcMetrics metrics;

    assert(metrics.active_connections() == 0);
    assert(metrics.total_requests() == 0);
    assert(metrics.total_responses() == 0);
    assert(metrics.success_requests() == 0);
    assert(metrics.failed_requests() == 0);
    assert(metrics.timeout_requests() == 0);
    assert(metrics.rejected_requests() == 0);
    assert(metrics.protocol_error_total() == 0);
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
    assert(ContainsLine(text, "minirpc_protocol_error_total 0"));
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
