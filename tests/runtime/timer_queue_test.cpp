#include <cassert>
#include <chrono>
#include <thread>
#include <vector>

#include "minirpc/runtime/timer_queue.h"

namespace {

using namespace std::chrono_literals;

void TestSleepForSuspendsAndResumes() {
    minirpc::Scheduler scheduler;
    minirpc::TimerQueue timers(&scheduler);
    std::vector<int> events;

    scheduler.Spawn([&] {
        events.push_back(1);
        assert(timers.SleepFor(10ms));
        events.push_back(3);
    });
    scheduler.Spawn([&] {
        events.push_back(2);
    });

    scheduler.Run();
    assert((events == std::vector<int>{1, 2}));
    assert(scheduler.ready_count() == 0);
    assert(timers.size() == 1);

    std::this_thread::sleep_for(20ms);
    assert(timers.DrainExpired() == 1);
    assert(scheduler.ready_count() == 1);
    scheduler.Run();

    assert((events == std::vector<int>{1, 2, 3}));
    assert(timers.empty());
}

void TestTimersExpireByDeadlineOrder() {
    minirpc::Scheduler scheduler;
    minirpc::TimerQueue timers(&scheduler);
    std::vector<int> events;

    minirpc::Coroutine* slow = scheduler.Spawn([&] {
        events.push_back(1);
        assert(timers.SleepFor(30ms));
        events.push_back(4);
    });
    minirpc::Coroutine* fast = scheduler.Spawn([&] {
        events.push_back(2);
        assert(timers.SleepFor(5ms));
        events.push_back(3);
    });

    scheduler.Run();
    assert((events == std::vector<int>{1, 2}));
    assert(!slow->Finished());
    assert(!fast->Finished());
    assert(timers.size() == 2);

    std::this_thread::sleep_for(10ms);
    assert(timers.DrainExpired() == 1);
    scheduler.Run();
    assert((events == std::vector<int>{1, 2, 3}));
    assert(!slow->Finished());
    assert(fast->Finished());

    std::this_thread::sleep_for(30ms);
    assert(timers.DrainExpired() == 1);
    scheduler.Run();
    assert((events == std::vector<int>{1, 2, 3, 4}));
    assert(slow->Finished());
}

void TestScheduleAfterExternalCoroutine() {
    minirpc::Scheduler scheduler;
    minirpc::TimerQueue timers(&scheduler);
    std::vector<int> events;

    minirpc::Coroutine* coroutine = scheduler.Spawn([&] {
        events.push_back(1);
        minirpc::Scheduler::SuspendCurrent();
        events.push_back(2);
    });

    scheduler.Run();
    assert((events == std::vector<int>{1}));
    assert(!coroutine->Finished());
    assert(timers.ScheduleAfter(coroutine, 5ms));
    assert(timers.TimeUntilNext().has_value());

    std::this_thread::sleep_for(10ms);
    assert(timers.DrainExpired() == 1);
    scheduler.Run();
    assert((events == std::vector<int>{1, 2}));
    assert(coroutine->Finished());
}

void TestSleepOutsideCoroutineFails() {
    minirpc::Scheduler scheduler;
    minirpc::TimerQueue timers(&scheduler);
    assert(!timers.SleepFor(1ms));
    assert(timers.empty());
}

}  // namespace

int main() {
    TestSleepForSuspendsAndResumes();
    TestTimersExpireByDeadlineOrder();
    TestScheduleAfterExternalCoroutine();
    TestSleepOutsideCoroutineFails();
    return 0;
}
