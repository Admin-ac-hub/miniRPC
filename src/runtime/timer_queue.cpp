#include "minirpc/runtime/timer_queue.h"

namespace minirpc {

TimerQueue::TimerQueue(Scheduler* scheduler)
    : scheduler_(scheduler),
      next_sequence_(1) {}

void TimerQueue::SetScheduler(Scheduler* scheduler) noexcept {
    scheduler_ = scheduler;
}

bool TimerQueue::ScheduleAt(Coroutine* coroutine, TimePoint deadline) {
    if (coroutine == nullptr || coroutine->Finished()) {
        return false;
    }
    timers_.push(Timer{deadline, next_sequence_++, coroutine});
    return true;
}

bool TimerQueue::ScheduleAfter(Coroutine* coroutine, Duration delay) {
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
    Coroutine* coroutine = Coroutine::Current();
    Scheduler* scheduler = ResolveScheduler();
    if (coroutine == nullptr || scheduler == nullptr) {
        return false;
    }
    if (!ScheduleAt(coroutine, deadline)) {
        return false;
    }
    Scheduler::SuspendCurrent();
    return true;
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
        if (timer.coroutine != nullptr && !timer.coroutine->Finished()) {
            scheduler->Schedule(timer.coroutine);
            ++drained;
        }
    }
    return drained;
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
