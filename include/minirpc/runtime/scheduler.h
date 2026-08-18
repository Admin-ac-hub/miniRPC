#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "minirpc/runtime/coroutine.h"

namespace minirpc {

class Scheduler {
public:
    using Task = std::function<void()>;

    explicit Scheduler(std::size_t default_stack_size = kDefaultCoroutineStackSize);
    ~Scheduler();

    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;

    CoroutineHandle Spawn(Task task);
    void Schedule(CoroutineHandle handle);
    void Run();

    std::size_t ready_count() const noexcept;
    std::size_t coroutine_count() const noexcept;
    bool Contains(CoroutineHandle handle) const noexcept;

    static Scheduler* Current() noexcept;
    static void YieldCurrent() noexcept;
    static void SuspendCurrent() noexcept;

private:
    Coroutine* Resolve(CoroutineHandle handle) noexcept;
    const Coroutine* Resolve(CoroutineHandle handle) const noexcept;
    void Enqueue(CoroutineHandle handle);
    void ReapFinished();
    void YieldRunning(bool requeue) noexcept;

    std::size_t default_stack_size_;
    std::unordered_map<std::uint64_t, std::unique_ptr<Coroutine>> coroutines_;
    std::deque<CoroutineHandle> ready_;
    std::unordered_set<std::uint64_t> ready_ids_;
    Coroutine* running_;
    bool requeue_running_;
    std::uint64_t next_coroutine_id_;
};

}  // namespace minirpc
