#include "minirpc/runtime/thread_pool.h"

#include <utility>

namespace minirpc {

ThreadPool::ThreadPool(std::size_t worker_count, std::size_t max_queue_size)
    : worker_count_(worker_count == 0 ? 1 : worker_count),
      max_queue_size_(max_queue_size == 0 ? 1 : max_queue_size),
      stopping_(false),
      started_(false) {}

ThreadPool::~ThreadPool() {
    Stop();
}

void ThreadPool::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) {
        return;
    }
    stopping_ = false;
    started_ = true;
    workers_.reserve(worker_count_);
    for (std::size_t i = 0; i < worker_count_; ++i) {
        workers_.emplace_back([this] { WorkerLoop(); });
    }
}

void ThreadPool::Stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) {
            return;
        }
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    started_ = false;
}

bool ThreadPool::Post(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_ || !started_ || tasks_.size() >= max_queue_size_) {
            return false;
        }
        tasks_.push(std::move(task));
    }
    cv_.notify_one();
    return true;
}

std::size_t ThreadPool::queued_tasks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_.size();
}

std::size_t ThreadPool::max_queue_size() const {
    return max_queue_size_;
}

void ThreadPool::WorkerLoop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        task();
    }
}

}  // namespace minirpc
