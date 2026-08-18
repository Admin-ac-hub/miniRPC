#include "minirpc/runtime/timer_queue.h"

namespace minirpc {

TimerQueue::TimerQueue(Scheduler* scheduler)
    : scheduler_(scheduler),
      next_sequence_(1) {}

void TimerQueue::SetScheduler(Scheduler* scheduler) noexcept {
    scheduler_ = scheduler;
}

bool TimerQueue::ScheduleAt(CoroutineHandle coroutine, TimePoint deadline) {
    Scheduler* scheduler = ResolveScheduler();
    if (stopped_ || !coroutine || scheduler == nullptr || !scheduler->Contains(coroutine)) {
        return false;
    }
    timers_.push(Timer{deadline, next_sequence_++, coroutine});
    return true;
}

bool TimerQueue::ScheduleAfter(CoroutineHandle coroutine, Duration delay) {
    if (delay < Duration::zero()) {
        delay = Duration::zero();
    }
    return ScheduleAt(coroutine, Clock::now() + delay);
}

bool TimerQueue::SleepFor(Duration delay) {
    if (delay < Duration::zero()) {
        delay = Duration::zero();
    }
    return SleepUntil(Clock::now() + delay);
}

bool TimerQueue::SleepUntil(TimePoint deadline) {
    Coroutine* current = Coroutine::Current();
    Scheduler* scheduler = ResolveScheduler();
    if (current == nullptr || scheduler == nullptr) {
        return false;
    }
    const CoroutineHandle coroutine = current->handle();
    if (!sleeping_.insert(coroutine.id).second) {
        return false;
    }
    if (!ScheduleAt(coroutine, deadline)) {
        sleeping_.erase(coroutine.id);
        return false;
    }
    Scheduler::SuspendCurrent();
    sleeping_.erase(coroutine.id);
    return cancelled_.erase(coroutine.id) == 0;
}

std::size_t TimerQueue::DrainExpired(TimePoint now) {
    Scheduler* scheduler = ResolveScheduler();
    if (scheduler == nullptr) {
        return 0;
    }

    std::size_t drained = 0;
    while (!timers_.empty() && timers_.top().deadline <= now) {
        Timer timer = timers_.top();
        timers_.pop();
        if (scheduler->Contains(timer.coroutine)) {
            scheduler->Schedule(timer.coroutine);
            ++drained;
        }
    }
    return drained;
}

std::size_t TimerQueue::CancelAll() {
    stopped_ = true;
    Scheduler* scheduler = ResolveScheduler();
    if (scheduler == nullptr) {
        return 0;
    }

    std::size_t cancelled = 0;
    while (!timers_.empty()) {
        const Timer timer = timers_.top();
        timers_.pop();
        if (!scheduler->Contains(timer.coroutine)) {
            continue;
        }
        if (sleeping_.count(timer.coroutine.id) > 0) {
            cancelled_.insert(timer.coroutine.id);
        }
        scheduler->Schedule(timer.coroutine);
        ++cancelled;
    }
    return cancelled;
}

std::optional<TimerQueue::Duration> TimerQueue::TimeUntilNext(TimePoint now) const {
    if (timers_.empty()) {
        return std::nullopt;
    }
    if (timers_.top().deadline <= now) {
        return Duration::zero();
    }
    return timers_.top().deadline - now;
}

std::size_t TimerQueue::size() const noexcept {
    return timers_.size();
}

bool TimerQueue::empty() const noexcept {
    return timers_.empty();
}

bool TimerQueue::LaterDeadline::operator()(const Timer& lhs, const Timer& rhs) const noexcept {
    if (lhs.deadline == rhs.deadline) {
        return lhs.sequence > rhs.sequence;
    }
    return lhs.deadline > rhs.deadline;
}

Scheduler* TimerQueue::ResolveScheduler() const noexcept {
    if (scheduler_ != nullptr) {
        return scheduler_;
    }
    return Scheduler::Current();
}

}  // namespace minirpc
