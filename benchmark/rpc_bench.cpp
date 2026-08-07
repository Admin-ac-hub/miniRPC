#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sys/resource.h>
#include <sys/utsname.h>

#include "minirpc/client/rpc_client.h"
#include "minirpc/core/status.h"
#include "minirpc/server/coroutine_rpc_server.h"
#include "minirpc/server/rpc_server.h"

namespace {

#ifndef MINIRPC_GIT_REVISION
#define MINIRPC_GIT_REVISION "unknown"
#endif

#ifndef MINIRPC_LIBURING_VERSION
#define MINIRPC_LIBURING_VERSION "unknown"
#endif

#ifndef MINIRPC_TCP_SERVER_BACKEND_NAME
#define MINIRPC_TCP_SERVER_BACKEND_NAME "unknown"
#endif

struct Options {
    std::string host = "127.0.0.1";
    std::string server = "both";
    std::size_t connections = 16;
    std::size_t requests = 1000;
    std::size_t runs = 5;
    std::size_t payload_size = 16;
    std::vector<std::size_t> payload_sizes;
    int handler_delay_ms = 0;
    int timeout_ms = 3000;
    uint16_t port = 19700;
};

struct CpuUsage {
    double user_seconds = 0.0;
    double system_seconds = 0.0;
};

struct BenchResult {
    std::string server;
    std::size_t connections = 0;
    std::size_t requests = 0;
    std::size_t success = 0;
    std::size_t failed = 0;
    double seconds = 0.0;
    double qps = 0.0;
    uint64_t p50_us = 0;
    uint64_t p95_us = 0;
    uint64_t p99_us = 0;
    uint64_t max_us = 0;
    double avg_ms = 0.0;
    CpuUsage cpu;
    uint64_t rejected_requests = 0;
    uint64_t timeout_requests = 0;
    std::map<int32_t, std::size_t> status_counts;
};

void PrintUsage(const char* argv0) {
    std::cout << "Usage: " << argv0 << " [options]\n"
              << "  --host HOST                        default: 127.0.0.1\n"
              << "  --server reactor|coroutine|both   default: both\n"
              << "  --connections N                   default: 16\n"
              << "  --requests N                      default: 1000\n"
              << "  --runs N                          default: 5 (3-5 recommended)\n"
              << "  --payload_size N                  default: 16\n"
              << "  --payload-sizes A,B,C             run multiple payload sizes\n"
              << "  --handler-delay-ms N              default: 0\n"
              << "  --timeout-ms N                    default: 3000\n"
              << "  --port N                          default: 19700\n";
}

bool ParseSize(const std::string& value, std::size_t* out) {
    if (value.empty() || value.front() == '-') {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (errno == ERANGE || end == value.c_str() || *end != '\0' || parsed > std::numeric_limits<std::size_t>::max()) {
        return false;
    }
    *out = static_cast<std::size_t>(parsed);
    return true;
}

bool ParseInt(const std::string& value, int* out) {
    errno = 0;
    char* end = nullptr;
    const long parsed = std::strtol(value.c_str(), &end, 10);
    if (errno == ERANGE || end == value.c_str() || *end != '\0' || parsed < std::numeric_limits<int>::min() ||
        parsed > std::numeric_limits<int>::max()) {
        return false;
    }
    *out = static_cast<int>(parsed);
    return true;
}

bool ParseSizeList(const std::string& value, std::vector<std::size_t>* out) {
    std::stringstream input(value);
    std::string item;
    std::vector<std::size_t> parsed;
    while (std::getline(input, item, ',')) {
        std::size_t size = 0;
        if (item.empty() || !ParseSize(item, &size) || size == 0) {
            return false;
        }
        parsed.push_back(size);
    }
    if (parsed.empty()) {
        return false;
    }
    *out = std::move(parsed);
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        if (i + 1 >= argc) {
            std::cerr << "missing value for " << arg << "\n";
            return false;
        }
        const std::string value = argv[++i];
        if (arg == "--host") {
            options->host = value;
        } else if (arg == "--server") {
            options->server = value;
        } else if (arg == "--connections") {
            if (!ParseSize(value, &options->connections))
                return false;
        } else if (arg == "--requests") {
            if (!ParseSize(value, &options->requests))
                return false;
        } else if (arg == "--runs") {
            if (!ParseSize(value, &options->runs))
                return false;
        } else if (arg == "--payload_size" || arg == "--payload-size") {
            if (!ParseSize(value, &options->payload_size))
                return false;
        } else if (arg == "--payload-sizes") {
            if (!ParseSizeList(value, &options->payload_sizes))
                return false;
        } else if (arg == "--handler_delay_ms" || arg == "--handler-delay-ms") {
            if (!ParseInt(value, &options->handler_delay_ms))
                return false;
        } else if (arg == "--timeout_ms" || arg == "--timeout-ms") {
            if (!ParseInt(value, &options->timeout_ms))
                return false;
        } else if (arg == "--port") {
            std::size_t port = 0;
            if (!ParseSize(value, &port) || port == 0 || port > 65535)
                return false;
            options->port = static_cast<uint16_t>(port);
        } else {
            std::cerr << "unknown option: " << arg << "\n";
            return false;
        }
    }

    if (options->server != "reactor" && options->server != "coroutine" && options->server != "both") {
        std::cerr << "--server must be reactor, coroutine, or both\n";
        return false;
    }
    if (options->connections == 0 || options->requests == 0 || options->runs == 0 || options->timeout_ms <= 0 ||
        options->handler_delay_ms < 0) {
        std::cerr << "connections, requests, runs, and timeout must be positive; "
                     "handler delay cannot be negative\n";
        return false;
    }
    if (options->server == "both" && options->port == std::numeric_limits<uint16_t>::max()) {
        std::cerr << "--port must be at most 65534 when --server both is used\n";
        return false;
    }
    return true;
}

CpuUsage ReadCpuUsage() {
    rusage usage{};
    (void)getrusage(RUSAGE_SELF, &usage);
    CpuUsage result;
    result.user_seconds =
        static_cast<double>(usage.ru_utime.tv_sec) + static_cast<double>(usage.ru_utime.tv_usec) / 1000000.0;
    result.system_seconds =
        static_cast<double>(usage.ru_stime.tv_sec) + static_cast<double>(usage.ru_stime.tv_usec) / 1000000.0;
    return result;
}

std::string KernelVersion() {
    utsname info{};
    return ::uname(&info) == 0 ? info.release : "unknown";
}

const char* CompilerVersion() {
#if defined(__clang__)
    return __clang_version__;
#elif defined(__GNUC__)
    return __VERSION__;
#else
    return "unknown";
#endif
}

CpuUsage DeltaCpu(CpuUsage after, CpuUsage before) {
    return {after.user_seconds - before.user_seconds, after.system_seconds - before.system_seconds};
}

uint64_t Percentile(const std::vector<uint64_t>& sorted, double percentile) {
    if (sorted.empty()) {
        return 0;
    }
    const double index = percentile * static_cast<double>(sorted.size() - 1);
    return sorted[static_cast<std::size_t>(index + 0.5)];
}

minirpc::RpcResponse MakeEchoResponse(const minirpc::RpcRequest& request, int handler_delay_ms) {
    if (handler_delay_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(handler_delay_ms));
    }
    minirpc::RpcResponse response;
    response.request_id = request.request_id;
    response.status_code = static_cast<int32_t>(minirpc::StatusCode::kOk);
    response.payload = request.payload;
    return response;
}

template <typename Server> BenchResult RunOnce(const std::string& server_name, const Options& options, uint16_t port) {
    Server server;
    server.RegisterService("EchoService", "Echo", [&](const minirpc::RpcRequest& request) {
        return MakeEchoResponse(request, options.handler_delay_ms);
    });

    const minirpc::Endpoint endpoint{options.host, port};
    const minirpc::Status status = server.Start(endpoint);
    if (!status.ok()) {
        std::cerr << "failed to start " << server_name << " server: " << status.ToString() << "\n";
        std::exit(2);
    }
    std::vector<uint64_t> latencies(options.requests);
    std::atomic<std::size_t> next_request{0};
    std::atomic<std::size_t> success{0};
    std::atomic<std::size_t> failed{0};
    std::atomic<std::size_t> ready_connections{0};
    std::atomic<std::size_t> warmup_failures{0};
    std::atomic<bool> start_measurement{false};
    std::atomic<bool> abort_measurement{false};
    std::mutex status_mutex;
    std::mutex finish_mutex;
    std::condition_variable finish_cv;
    std::size_t finished_connections = 0;
    bool measurement_finished = false;
    std::chrono::steady_clock::time_point measurement_end;
    CpuUsage cpu_after;
    std::map<int32_t, std::size_t> status_counts;
    const std::string payload(options.payload_size, 'x');

    std::vector<std::thread> workers;
    workers.reserve(options.connections);
    for (std::size_t i = 0; i < options.connections; ++i) {
        workers.emplace_back([&, i] {
            (void)i;
            minirpc::RpcClient client(endpoint);
            bool warmed_up = false;
            for (int attempt = 0; attempt < 20 && !warmed_up; ++attempt) {
                minirpc::RpcResponse response =
                    client.Call("EchoService", "Echo", payload, std::chrono::milliseconds(options.timeout_ms));
                warmed_up = response.status_code == static_cast<int32_t>(minirpc::StatusCode::kOk) &&
                            response.payload == payload;
                if (!warmed_up) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
            if (!warmed_up) {
                warmup_failures.fetch_add(1, std::memory_order_relaxed);
            }
            ready_connections.fetch_add(1, std::memory_order_release);
            while (!start_measurement.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            if (abort_measurement.load(std::memory_order_acquire)) {
                client.Close();
                return;
            }

            while (true) {
                const std::size_t request_index = next_request.fetch_add(1, std::memory_order_relaxed);
                if (request_index >= options.requests) {
                    break;
                }
                const auto call_start = std::chrono::steady_clock::now();
                minirpc::RpcResponse response =
                    client.Call("EchoService", "Echo", payload, std::chrono::milliseconds(options.timeout_ms));
                const auto call_end = std::chrono::steady_clock::now();
                latencies[request_index] = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(call_end - call_start).count());
                if (response.status_code == static_cast<int32_t>(minirpc::StatusCode::kOk) &&
                    response.payload == payload) {
                    success.fetch_add(1, std::memory_order_relaxed);
                } else {
                    failed.fetch_add(1, std::memory_order_relaxed);
                }
                {
                    std::lock_guard<std::mutex> lock(status_mutex);
                    ++status_counts[response.status_code];
                }
            }

            {
                std::unique_lock<std::mutex> lock(finish_mutex);
                ++finished_connections;
                if (finished_connections == options.connections) {
                    measurement_end = std::chrono::steady_clock::now();
                    cpu_after = ReadCpuUsage();
                    measurement_finished = true;
                    finish_cv.notify_all();
                } else {
                    finish_cv.wait(lock, [&] { return measurement_finished; });
                }
            }
            client.Close();
        });
    }

    while (ready_connections.load(std::memory_order_acquire) < options.connections) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const std::size_t failed_warmups = warmup_failures.load(std::memory_order_relaxed);
    if (failed_warmups != 0) {
        abort_measurement.store(true, std::memory_order_release);
        start_measurement.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }
        server.Stop();
        std::cerr << "failed to warm up " << failed_warmups << " of " << options.connections << " connections for "
                  << server_name << "\n";
        std::exit(2);
    }

    const uint64_t rejected_before = server.metrics().rejected_requests();
    const uint64_t timeout_before = server.metrics().timeout_requests();
    const CpuUsage cpu_before = ReadCpuUsage();
    const auto start = std::chrono::steady_clock::now();
    start_measurement.store(true, std::memory_order_release);

    for (auto& worker : workers) {
        worker.join();
    }

    std::sort(latencies.begin(), latencies.end());
    uint64_t total_latency_us = 0;
    for (uint64_t latency : latencies) {
        total_latency_us += latency;
    }
    BenchResult result;
    result.server = server_name;
    result.connections = options.connections;
    result.requests = options.requests;
    result.success = success.load(std::memory_order_relaxed);
    result.failed = failed.load(std::memory_order_relaxed);
    result.seconds = std::chrono::duration<double>(measurement_end - start).count();
    result.qps = result.seconds > 0.0 ? static_cast<double>(options.requests) / result.seconds : 0.0;
    result.p50_us = Percentile(latencies, 0.50);
    result.p95_us = Percentile(latencies, 0.95);
    result.p99_us = Percentile(latencies, 0.99);
    result.max_us = latencies.empty() ? 0 : latencies.back();
    result.avg_ms = options.requests == 0
                        ? 0.0
                        : static_cast<double>(total_latency_us) / static_cast<double>(options.requests) / 1000.0;
    result.cpu = DeltaCpu(cpu_after, cpu_before);
    result.rejected_requests = server.metrics().rejected_requests() - rejected_before;
    result.timeout_requests = server.metrics().timeout_requests() - timeout_before;
    result.status_counts = status_counts;

    server.Stop();
    return result;
}

template <typename T, typename Getter> T MedianOf(const std::vector<BenchResult>& results, Getter getter) {
    std::vector<T> values;
    values.reserve(results.size());
    for (const auto& result : results) {
        values.push_back(getter(result));
    }
    if (values.empty()) {
        return T{};
    }
    std::sort(values.begin(), values.end());
    const std::size_t middle = values.size() / 2;
    if (values.size() % 2 != 0) {
        return values[middle];
    }
    return values[middle - 1] + (values[middle] - values[middle - 1]) / static_cast<T>(2);
}

template <typename T, typename Getter> T SumOf(const std::vector<BenchResult>& results, Getter getter) {
    T sum{};
    for (const auto& result : results) {
        sum += getter(result);
    }
    return sum;
}

BenchResult SummaryResult(const std::vector<BenchResult>& results) {
    BenchResult summary;
    if (results.empty()) {
        return summary;
    }

    summary.server = results.front().server;
    summary.connections = results.front().connections;
    summary.requests = results.front().requests;
    summary.success = SumOf<std::size_t>(results, [](const BenchResult& result) { return result.success; });
    summary.failed = SumOf<std::size_t>(results, [](const BenchResult& result) { return result.failed; });
    summary.seconds = MedianOf<double>(results, [](const BenchResult& result) { return result.seconds; });
    summary.qps = MedianOf<double>(results, [](const BenchResult& result) { return result.qps; });
    summary.p50_us = MedianOf<uint64_t>(results, [](const BenchResult& result) { return result.p50_us; });
    summary.p95_us = MedianOf<uint64_t>(results, [](const BenchResult& result) { return result.p95_us; });
    summary.p99_us = MedianOf<uint64_t>(results, [](const BenchResult& result) { return result.p99_us; });
    summary.max_us = MedianOf<uint64_t>(results, [](const BenchResult& result) { return result.max_us; });
    summary.avg_ms = MedianOf<double>(results, [](const BenchResult& result) { return result.avg_ms; });
    summary.cpu.user_seconds =
        MedianOf<double>(results, [](const BenchResult& result) { return result.cpu.user_seconds; });
    summary.cpu.system_seconds =
        MedianOf<double>(results, [](const BenchResult& result) { return result.cpu.system_seconds; });
    summary.rejected_requests =
        SumOf<uint64_t>(results, [](const BenchResult& result) { return result.rejected_requests; });
    summary.timeout_requests =
        SumOf<uint64_t>(results, [](const BenchResult& result) { return result.timeout_requests; });

    std::map<int32_t, bool> statuses;
    for (const auto& result : results) {
        for (const auto& [status, count] : result.status_counts) {
            (void)count;
            statuses.emplace(status, true);
        }
    }
    for (const auto& [status, present] : statuses) {
        (void)present;
        summary.status_counts[status] = SumOf<std::size_t>(results, [status](const BenchResult& result) {
            const auto it = result.status_counts.find(status);
            return it == result.status_counts.end() ? 0 : it->second;
        });
    }
    return summary;
}

void PrintDetailedSummary(const BenchResult& result, std::size_t runs) {
    std::cout << "\n[" << result.server << " summary, runs=" << runs << "]\n";
    std::cout << "requests_per_run: " << result.requests << '\n';
    std::cout << "success_sum: " << result.success << '\n';
    std::cout << "failed_sum: " << result.failed << '\n';
    std::cout << "rejected_sum: " << result.rejected_requests << '\n';
    std::cout << "timeout_sum: " << result.timeout_requests << '\n';
    std::cout << "duration_median_ms: " << std::fixed << std::setprecision(2) << result.seconds * 1000.0 << '\n';
    std::cout << "qps_median: " << std::fixed << std::setprecision(1) << result.qps << '\n';
    std::cout << "avg_latency_median_ms: " << std::fixed << std::setprecision(3) << result.avg_ms << '\n';
    std::cout << "p50_latency_median_ms: " << std::fixed << std::setprecision(3)
              << static_cast<double>(result.p50_us) / 1000.0 << '\n';
    std::cout << "p95_latency_median_ms: " << std::fixed << std::setprecision(3)
              << static_cast<double>(result.p95_us) / 1000.0 << '\n';
    std::cout << "p99_latency_median_ms: " << std::fixed << std::setprecision(3)
              << static_cast<double>(result.p99_us) / 1000.0 << '\n';
    std::cout << "max_latency_median_ms: " << std::fixed << std::setprecision(3)
              << static_cast<double>(result.max_us) / 1000.0 << '\n';
}

void PrintResultHeader(bool summary) {
    std::cout << std::left << std::setw(12) << "server" << std::right << std::setw(8) << (summary ? "runs" : "run")
              << std::setw(8) << "conn" << std::setw(10) << (summary ? "req/run" : "req") << std::setw(12)
              << (summary ? "qps_med" : "qps") << std::setw(10) << (summary ? "p50us_med" : "p50us")
              << std::setw(10) << (summary ? "p95us_med" : "p95us") << std::setw(10)
              << (summary ? "p99us_med" : "p99us") << std::setw(10) << (summary ? "maxus_med" : "maxus")
              << std::setw(10) << (summary ? "fail_sum" : "failed") << std::setw(10)
              << (summary ? "usr_s_med" : "cpu_usr") << std::setw(10) << (summary ? "sys_s_med" : "cpu_sys")
              << std::setw(10) << (summary ? "rej_sum" : "rejected") << std::setw(10)
              << (summary ? "tout_sum" : "timeout") << (summary ? "  statuses_sum" : "  statuses") << '\n';
}

void PrintResult(const BenchResult& result, std::size_t run_or_run_count) {
    std::cout << std::left << std::setw(12) << result.server << std::right << std::setw(8) << run_or_run_count
              << std::setw(8) << result.connections << std::setw(10) << result.requests << std::setw(12)
              << static_cast<uint64_t>(result.qps) << std::setw(10) << result.p50_us << std::setw(10) << result.p95_us
              << std::setw(10) << result.p99_us << std::setw(10) << result.max_us << std::setw(10) << result.failed
              << std::setw(10) << std::fixed << std::setprecision(3) << result.cpu.user_seconds << std::setw(10)
              << std::fixed << std::setprecision(3) << result.cpu.system_seconds << std::setw(10)
              << result.rejected_requests << std::setw(10) << result.timeout_requests << "  ";
    bool first = true;
    for (const auto& [status, count] : result.status_counts) {
        if (!first) {
            std::cout << ",";
        }
        first = false;
        std::cout << status << ":" << count;
    }
    std::cout << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseOptions(argc, argv, &options)) {
        PrintUsage(argv[0]);
        return 1;
    }

    std::vector<std::size_t> payload_sizes = options.payload_sizes;
    if (payload_sizes.empty()) {
        payload_sizes.push_back(options.payload_size);
    }

    std::cout << "commit=" << MINIRPC_GIT_REVISION
              << " kernel=" << KernelVersion()
              << " compiler=\"" << CompilerVersion() << '"'
              << " tcp_backend=" << MINIRPC_TCP_SERVER_BACKEND_NAME
              << " coroutine_io_backend=epoll"
              << " liburing=" << MINIRPC_LIBURING_VERSION << '\n';

    for (std::size_t payload_size : payload_sizes) {
        Options run_options = options;
        run_options.payload_size = payload_size;
        std::vector<BenchResult> reactor_results;
        std::vector<BenchResult> coroutine_results;
        reactor_results.reserve(run_options.runs);
        coroutine_results.reserve(run_options.runs);

        const auto run_reactor = [&](std::size_t run) {
            std::cerr << "running payload_size=" << run_options.payload_size << " server=reactor run=" << run << '/'
                      << run_options.runs << '\n';
            reactor_results.push_back(RunOnce<minirpc::RpcServer>("reactor", run_options, run_options.port));
        };
        const auto run_coroutine = [&](std::size_t run) {
            const uint16_t port =
                run_options.server == "both" ? static_cast<uint16_t>(run_options.port + 1) : run_options.port;
            std::cerr << "running payload_size=" << run_options.payload_size << " server=coroutine run=" << run << '/'
                      << run_options.runs << '\n';
            coroutine_results.push_back(RunOnce<minirpc::CoroutineRpcServer>("coroutine", run_options, port));
        };

        for (std::size_t run = 1; run <= run_options.runs; ++run) {
            if (run_options.server == "reactor") {
                run_reactor(run);
            } else if (run_options.server == "coroutine") {
                run_coroutine(run);
            } else if (run % 2 != 0) {
                run_reactor(run);
                run_coroutine(run);
            } else {
                run_coroutine(run);
                run_reactor(run);
            }
        }

        std::cout << "host=" << run_options.host << " payload_size=" << run_options.payload_size
                  << " connections=" << run_options.connections << " requests_per_run=" << run_options.requests
                  << " runs=" << run_options.runs << " handler_delay_ms=" << run_options.handler_delay_ms
                  << " timeout_ms=" << run_options.timeout_ms << "\n";

        std::cout << "\nraw runs:\n";
        PrintResultHeader(false);
        for (std::size_t run = 0; run < run_options.runs; ++run) {
            if (!reactor_results.empty()) {
                PrintResult(reactor_results[run], run + 1);
            }
            if (!coroutine_results.empty()) {
                PrintResult(coroutine_results[run], run + 1);
            }
        }

        std::vector<BenchResult> summaries;
        if (!reactor_results.empty()) {
            summaries.push_back(SummaryResult(reactor_results));
        }
        if (!coroutine_results.empty()) {
            summaries.push_back(SummaryResult(coroutine_results));
        }
        std::cout << "\nsummary (performance=median, counters=sum across " << run_options.runs << " runs):\n";
        PrintResultHeader(true);
        for (const auto& summary : summaries) {
            PrintResult(summary, run_options.runs);
        }
        for (const auto& summary : summaries) {
            PrintDetailedSummary(summary, run_options.runs);
        }
    }

    return 0;
}
