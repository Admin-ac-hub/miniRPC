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
    assert(scheduler.coroutine_count() == 0);
    assert(minirpc::Scheduler::Current() == nullptr);
    assert(minirpc::Coroutine::Current() == nullptr);
}

void TestSuspendAndExternalSchedule() {
    minirpc::Scheduler scheduler;
    std::vector<int> events;
    const minirpc::CoroutineHandle coroutine = scheduler.Spawn([&] {
        events.push_back(1);
        minirpc::Scheduler::SuspendCurrent();
        events.push_back(2);
    });

    scheduler.Run();
    assert((events == std::vector<int>{1}));
    assert(scheduler.ready_count() == 0);
    assert(scheduler.Contains(coroutine));

    scheduler.Schedule(coroutine);
    assert(scheduler.ready_count() == 1);
    scheduler.Run();
    assert((events == std::vector<int>{1, 2}));
    assert(!scheduler.Contains(coroutine));
}

void TestFinishedCoroutineIsNotScheduled() {
    minirpc::Scheduler scheduler;
    const minirpc::CoroutineHandle coroutine = scheduler.Spawn([] {});
    scheduler.Run();
    assert(!scheduler.Contains(coroutine));

    scheduler.Schedule(coroutine);
    assert(scheduler.ready_count() == 0);
}

void TestDuplicateScheduleOnlyResumesOnce() {
    minirpc::Scheduler scheduler;
    int resume_count = 0;
    const minirpc::CoroutineHandle coroutine = scheduler.Spawn([&] {
        minirpc::Scheduler::SuspendCurrent();
        ++resume_count;
        minirpc::Scheduler::SuspendCurrent();
        ++resume_count;
    });

    scheduler.Run();
    scheduler.Schedule(coroutine);
    scheduler.Schedule(coroutine);
    scheduler.Run();

    assert(resume_count == 1);
    assert(scheduler.ready_count() == 0);
    assert(scheduler.Contains(coroutine));

    scheduler.Schedule(coroutine);
    scheduler.Run();
    assert(resume_count == 2);
    assert(!scheduler.Contains(coroutine));
}

void TestFinishedCoroutinesAreReclaimed() {
    minirpc::Scheduler scheduler;
    for (int i = 0; i < 10000; ++i) {
        scheduler.Spawn([] {});
    }

    scheduler.Run();

    assert(scheduler.ready_count() == 0);
    assert(scheduler.coroutine_count() == 0);
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
    TestDuplicateScheduleOnlyResumesOnce();
    TestFinishedCoroutinesAreReclaimed();
    TestNestedSchedulerRestoresCurrentScheduler();
    return 0;
}
