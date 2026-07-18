#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <vector>

#include "minirpc/runtime/coroutine.h"

namespace minirpc {

class Scheduler {
public:
    using Task = std::function<void()>;

    explicit Scheduler(std::size_t default_stack_size = kDefaultCoroutineStackSize);
    ~Scheduler();

    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;

    Coroutine* Spawn(Task task);
    void Schedule(Coroutine* coroutine);
    void Run();

    std::size_t ready_count() const noexcept;
    std::size_t coroutine_count() const noexcept;

    static Scheduler* Current() noexcept;
    static void YieldCurrent() noexcept;
    static void SuspendCurrent() noexcept;

private:
    void YieldRunning(bool requeue) noexcept;

    std::size_t default_stack_size_;
    std::vector<std::unique_ptr<Coroutine>> coroutines_;
    std::deque<Coroutine*> ready_;
    Coroutine* running_;
    bool requeue_running_;
};

}  // namespace minirpc
