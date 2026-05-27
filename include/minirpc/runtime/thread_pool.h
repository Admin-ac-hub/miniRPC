#pragma once

#include <cstddef>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace minirpc {

class ThreadPool {
public:
    explicit ThreadPool(std::size_t worker_count = std::thread::hardware_concurrency(),
                        std::size_t max_queue_size = 10000);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void Start();
    void Stop();
    bool Post(std::function<void()> task);
    std::size_t queued_tasks() const;
    std::size_t max_queue_size() const;

private:
    void WorkerLoop();

    std::size_t worker_count_;
    std::size_t max_queue_size_;
    bool stopping_;
    bool started_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
};

}  // namespace minirpc
