#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <functional>
#include <mutex>
#include <sys/types.h>
#include <unordered_map>
#include <vector>

#include "minirpc/runtime/coroutine.h"
#include "minirpc/runtime/scheduler.h"
#include "minirpc/runtime/timer_queue.h"

namespace minirpc {

class CoroutineIoContext {
public:
    using Task = std::function<void()>;

    explicit CoroutineIoContext(std::size_t default_stack_size = kDefaultCoroutineStackSize);
    ~CoroutineIoContext();

    CoroutineIoContext(const CoroutineIoContext&) = delete;
    CoroutineIoContext& operator=(const CoroutineIoContext&) = delete;

    Coroutine* Spawn(Task task);
    void Schedule(Coroutine* coroutine);
    void Run();
    void Stop() noexcept;
    void AddExternalWait() noexcept;
    void CompleteExternalWait() noexcept;

    bool WaitReadable(int fd);
    bool WaitWritable(int fd);
    ssize_t Read(int fd, void* buffer, std::size_t size);
    ssize_t WriteAll(int fd, const void* buffer, std::size_t size);

    Scheduler& scheduler() noexcept;
    TimerQueue& timers() noexcept;

    std::size_t waiting_count() const noexcept;

    static CoroutineIoContext* Current() noexcept;

private:
    enum class WaitKind {
        kRead,
        kWrite,
    };

    struct FdWaiters {
        Coroutine* read = nullptr;
        Coroutine* write = nullptr;
        bool registered = false;
    };

    bool WaitFd(int fd, WaitKind kind);
    void WakeFd(int fd, std::uint32_t events);
    bool UpdateInterest(int fd, FdWaiters* waiters);
    void DrainWake();
    void DrainPendingReady();
    void Wake();
    bool HasPendingWork() const;
    int NextPollTimeoutMs() const;

    Scheduler scheduler_;
    TimerQueue timers_;
    int epoll_fd_;
    int wake_fd_;
    std::atomic<bool> stopping_;
    std::atomic<std::size_t> external_waits_;
    std::unordered_map<int, FdWaiters> waiters_;
    mutable std::mutex pending_mutex_;
    std::vector<Coroutine*> pending_ready_;
};

}  // namespace minirpc
