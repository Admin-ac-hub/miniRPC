#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "minirpc/observability/metrics.h"

namespace {

constexpr uint64_t kIterations = 5000000;
constexpr int kRounds = 5;

double Run(std::size_t writers, bool with_reader) {
    minirpc::RpcMetrics metrics;
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::atomic<std::size_t> ready{0};
    std::atomic<bool> finished{false};
    std::thread reader;
    if (with_reader) {
        reader = std::thread([&] {
            ready.fetch_add(1);
            gate.wait();
            while (!finished.load()) {
                (void)metrics.Snapshot();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }
    std::vector<std::thread> threads;
    for (std::size_t i = 0; i < writers; ++i) {
        threads.emplace_back([&] {
            ready.fetch_add(1);
            gate.wait();
            for (uint64_t j = 0; j < kIterations; ++j) {
                metrics.RecordLatency(std::chrono::microseconds(1 + j % 1000));
            }
        });
    }
    while (ready.load() != writers + (with_reader ? 1 : 0)) std::this_thread::yield();
    const auto start = std::chrono::steady_clock::now();
    release.set_value();
    for (auto& thread : threads) thread.join();
    const double seconds = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start).count();
    finished.store(true);
    if (reader.joinable()) reader.join();
    if (metrics.latency_samples() != writers * kIterations) return 0;
    return static_cast<double>(writers * kIterations) / seconds / 1000000.0;
}

}  // namespace

int main() {
    std::cout << "writers,snapshot_reader,round,million_samples_per_second\n";
    for (std::size_t writers : {1, 5}) {
        for (bool reader : {false, true}) {
            std::vector<double> rates;
            for (int round = 1; round <= kRounds; ++round) {
                const double rate = Run(writers, reader);
                if (rate == 0) return 1;
                rates.push_back(rate);
                std::cout << writers << ',' << reader << ',' << round << ','
                          << std::fixed << std::setprecision(3) << rate << '\n';
            }
            std::sort(rates.begin(), rates.end());
            std::cout << writers << ',' << reader << ",median," << rates[kRounds / 2] << '\n';
        }
    }
}
