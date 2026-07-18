#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <queue>
#include <vector>

#include "minirpc/runtime/coroutine.h"
#include "minirpc/runtime/scheduler.h"

namespace minirpc {

class TimerQueue {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;

    explicit TimerQueue(Scheduler* scheduler = nullptr);

    TimerQueue(const TimerQueue&) = delete;
    TimerQueue& operator=(const TimerQueue&) = delete;

    void SetScheduler(Scheduler* scheduler) noexcept;

    bool ScheduleAt(Coroutine* coroutine, TimePoint deadline);
    bool ScheduleAfter(Coroutine* coroutine, Duration delay);
    bool SleepFor(Duration delay);
    bool SleepUntil(TimePoint deadline);

    std::size_t DrainExpired(TimePoint now = Clock::now());
    std::optional<Duration> TimeUntilNext(TimePoint now = Clock::now()) const;
    std::size_t size() const noexcept;
    bool empty() const noexcept;

private:
    struct Timer {
        TimePoint deadline;
        std::uint64_t sequence;
        Coroutine* coroutine;
    };

    struct LaterDeadline {
        bool operator()(const Timer& lhs, const Timer& rhs) const noexcept;
    };

    Scheduler* ResolveScheduler() const noexcept;

    Scheduler* scheduler_;
    std::uint64_t next_sequence_;
    std::priority_queue<Timer, std::vector<Timer>, LaterDeadline> timers_;
};

}  // namespace minirpc
