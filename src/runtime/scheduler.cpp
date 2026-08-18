#include "minirpc/runtime/scheduler.h"

#include <utility>

namespace minirpc {
namespace {

thread_local Scheduler* t_current_scheduler = nullptr;

}  // namespace

Scheduler::Scheduler(std::size_t default_stack_size)
    : default_stack_size_(default_stack_size == 0 ? kDefaultCoroutineStackSize : default_stack_size),
      running_(nullptr),
      requeue_running_(false),
      next_coroutine_id_(1) {}

Scheduler::~Scheduler() = default;

CoroutineHandle Scheduler::Spawn(Task task) {
    const CoroutineHandle handle{next_coroutine_id_++, 1};
    auto coroutine = std::make_unique<Coroutine>(std::move(task), default_stack_size_);
    coroutine->SetHandle(handle);
    coroutines_.emplace(handle.id, std::move(coroutine));
    Enqueue(handle);
    return handle;
}

void Scheduler::Schedule(CoroutineHandle handle) {
    Coroutine* coroutine = Resolve(handle);
    if (coroutine == nullptr || coroutine->Finished()) {
        return;
    }
    Enqueue(handle);
}

void Scheduler::Run() {
    Scheduler* previous = t_current_scheduler;
    t_current_scheduler = this;

    while (!ready_.empty()) {
        const CoroutineHandle handle = ready_.front();
        ready_.pop_front();
        ready_ids_.erase(handle.id);
        Coroutine* coroutine = Resolve(handle);
        if (coroutine == nullptr || coroutine->Finished()) {
            continue;
        }

        running_ = coroutine;
        requeue_running_ = false;
        coroutine->Resume();
        if (!coroutine->Finished() && requeue_running_) {
            Enqueue(handle);
        }
        running_ = nullptr;
        requeue_running_ = false;
    }

    ReapFinished();
    t_current_scheduler = previous;
}

std::size_t Scheduler::ready_count() const noexcept {
    return ready_.size();
}

std::size_t Scheduler::coroutine_count() const noexcept {
    return coroutines_.size();
}

bool Scheduler::Contains(CoroutineHandle handle) const noexcept {
    return Resolve(handle) != nullptr;
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

Coroutine* Scheduler::Resolve(CoroutineHandle handle) noexcept {
    if (!handle) {
        return nullptr;
    }
    auto it = coroutines_.find(handle.id);
    if (it == coroutines_.end() || it->second->handle() != handle) {
        return nullptr;
    }
    return it->second.get();
}

const Coroutine* Scheduler::Resolve(CoroutineHandle handle) const noexcept {
    if (!handle) {
        return nullptr;
    }
    auto it = coroutines_.find(handle.id);
    if (it == coroutines_.end() || it->second->handle() != handle) {
        return nullptr;
    }
    return it->second.get();
}

void Scheduler::Enqueue(CoroutineHandle handle) {
    if (!handle || !ready_ids_.insert(handle.id).second) {
        return;
    }
    ready_.push_back(handle);
}

void Scheduler::ReapFinished() {
    for (auto it = coroutines_.begin(); it != coroutines_.end();) {
        if (it->second->Finished()) {
            ready_ids_.erase(it->first);
            it = coroutines_.erase(it);
        } else {
            ++it;
        }
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
