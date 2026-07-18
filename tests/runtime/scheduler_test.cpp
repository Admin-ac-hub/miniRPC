#include <cassert>
#include <vector>

#include "minirpc/runtime/scheduler.h"

namespace {

void TestRoundRobinYield() {
    minirpc::Scheduler scheduler;
    std::vector<int> events;

    scheduler.Spawn([&] {
        assert(minirpc::Scheduler::Current() == &scheduler);
        events.push_back(1);
        minirpc::Scheduler::YieldCurrent();
        events.push_back(3);
    });
    scheduler.Spawn([&] {
        assert(minirpc::Scheduler::Current() == &scheduler);
        events.push_back(2);
    });

    scheduler.Run();

    assert((events == std::vector<int>{1, 2, 3}));
    assert(scheduler.ready_count() == 0);
    assert(scheduler.coroutine_count() == 2);
    assert(minirpc::Scheduler::Current() == nullptr);
    assert(minirpc::Coroutine::Current() == nullptr);
}

void TestSuspendAndExternalSchedule() {
    minirpc::Scheduler scheduler;
    std::vector<int> events;
    minirpc::Coroutine* coroutine = scheduler.Spawn([&] {
        events.push_back(1);
        minirpc::Scheduler::SuspendCurrent();
        events.push_back(2);
    });

    scheduler.Run();
    assert((events == std::vector<int>{1}));
    assert(scheduler.ready_count() == 0);
    assert(!coroutine->Finished());

    scheduler.Schedule(coroutine);
    assert(scheduler.ready_count() == 1);
    scheduler.Run();
    assert((events == std::vector<int>{1, 2}));
    assert(coroutine->Finished());
}

void TestFinishedCoroutineIsNotScheduled() {
    minirpc::Scheduler scheduler;
    minirpc::Coroutine* coroutine = scheduler.Spawn([] {});
    scheduler.Run();
    assert(coroutine->Finished());

    scheduler.Schedule(coroutine);
    assert(scheduler.ready_count() == 0);
}

void TestNestedSchedulerRestoresCurrentScheduler() {
    minirpc::Scheduler outer;
    minirpc::Scheduler inner;
    std::vector<int> events;

    outer.Spawn([&] {
        assert(minirpc::Scheduler::Current() == &outer);
        events.push_back(1);
        inner.Spawn([&] {
            assert(minirpc::Scheduler::Current() == &inner);
            events.push_back(2);
        });
        inner.Run();
        assert(minirpc::Scheduler::Current() == &outer);
        events.push_back(3);
    });

    outer.Run();

    assert((events == std::vector<int>{1, 2, 3}));
    assert(minirpc::Scheduler::Current() == nullptr);
}

}  // namespace

int main() {
    TestRoundRobinYield();
    TestSuspendAndExternalSchedule();
    TestFinishedCoroutineIsNotScheduled();
    TestNestedSchedulerRestoresCurrentScheduler();
    return 0;
}
