#include "minirpc/runtime/scheduler.h"

#include <utility>

namespace minirpc {
namespace {

thread_local Scheduler* t_current_scheduler = nullptr;

}  // namespace

Scheduler::Scheduler(std::size_t default_stack_size)
    : default_stack_size_(default_stack_size == 0 ? kDefaultCoroutineStackSize : default_stack_size),
      running_(nullptr),
      requeue_running_(false) {}

Scheduler::~Scheduler() = default;

Coroutine* Scheduler::Spawn(Task task) {
    auto coroutine = std::make_unique<Coroutine>(std::move(task), default_stack_size_);
    Coroutine* raw = coroutine.get();
    coroutines_.push_back(std::move(coroutine));
    ready_.push_back(raw);
    return raw;
}

void Scheduler::Schedule(Coroutine* coroutine) {
    if (coroutine == nullptr || coroutine->Finished()) {
        return;
    }
    ready_.push_back(coroutine);
}

void Scheduler::Run() {
    Scheduler* previous = t_current_scheduler;
    t_current_scheduler = this;

    while (!ready_.empty()) {
        Coroutine* coroutine = ready_.front();
        ready_.pop_front();
        if (coroutine == nullptr || coroutine->Finished()) {
            continue;
        }

        running_ = coroutine;
        requeue_running_ = false;
        coroutine->Resume();
        if (!coroutine->Finished() && requeue_running_) {
            ready_.push_back(coroutine);
        }
        running_ = nullptr;
        requeue_running_ = false;
    }

    t_current_scheduler = previous;
}

std::size_t Scheduler::ready_count() const noexcept {
    return ready_.size();
}

std::size_t Scheduler::coroutine_count() const noexcept {
    return coroutines_.size();
}

Scheduler* Scheduler::Current() noexcept {
    return t_current_scheduler;
}

void Scheduler::YieldCurrent() noexcept {
    Scheduler* scheduler = Scheduler::Current();
    if (scheduler != nullptr) {
        scheduler->YieldRunning(true);
        return;
    }
    Coroutine* coroutine = Coroutine::Current();
    if (coroutine != nullptr) {
        coroutine->Yield();
    }
}

void Scheduler::SuspendCurrent() noexcept {
    Scheduler* scheduler = Scheduler::Current();
    if (scheduler != nullptr) {
        scheduler->YieldRunning(false);
        return;
    }
    Coroutine* coroutine = Coroutine::Current();
    if (coroutine != nullptr) {
        coroutine->Yield();
    }
}

void Scheduler::YieldRunning(bool requeue) noexcept {
    if (running_ == nullptr || Coroutine::Current() != running_) {
        return;
    }
    requeue_running_ = requeue;
    running_->Yield();
}

}  // namespace minirpc
