#include <cassert>
#include <memory>
#include <stdexcept>
#include <vector>

#include "minirpc/runtime/coroutine.h"

namespace {

void TestResumeYieldSequence() {
    std::vector<int> events;
    minirpc::Coroutine* coroutine = nullptr;
    minirpc::Coroutine co([&] {
        assert(minirpc::Coroutine::Current() == coroutine);
        events.push_back(1);
        coroutine->Yield();
        assert(minirpc::Coroutine::Current() == coroutine);
        events.push_back(2);
    });
    coroutine = &co;

    assert(co.state() == minirpc::Coroutine::State::kReady);
    co.Resume();
    assert((events == std::vector<int>{1}));
    assert(co.state() == minirpc::Coroutine::State::kSuspended);
    assert(!co.Finished());
    assert(minirpc::Coroutine::Current() == nullptr);

    co.Resume();
    assert((events == std::vector<int>{1, 2}));
    assert(co.Finished());
    assert(co.state() == minirpc::Coroutine::State::kFinished);
    assert(minirpc::Coroutine::Current() == nullptr);

    co.Resume();
    assert((events == std::vector<int>{1, 2}));
}

void TestMultipleCoroutinesKeepSeparateStacks() {
    std::vector<int> events;
    minirpc::Coroutine* first = nullptr;
    minirpc::Coroutine* second = nullptr;

    minirpc::Coroutine co1([&] {
        int local = 10;
        events.push_back(local);
        first->Yield();
        events.push_back(local + 1);
    });
    minirpc::Coroutine co2([&] {
        int local = 20;
        events.push_back(local);
        second->Yield();
        events.push_back(local + 1);
    });
    first = &co1;
    second = &co2;

    co1.Resume();
    co2.Resume();
    co1.Resume();
    co2.Resume();

    assert((events == std::vector<int>{10, 20, 11, 21}));
    assert(co1.Finished());
    assert(co2.Finished());
}

void TestNestedResumeReturnsToCallerCoroutine() {
    std::vector<int> events;
    minirpc::Coroutine* outer = nullptr;
    minirpc::Coroutine* inner = nullptr;

    minirpc::Coroutine inner_co([&] {
        assert(minirpc::Coroutine::Current() == inner);
        events.push_back(2);
        inner->Yield();
        events.push_back(4);
    });
    minirpc::Coroutine outer_co([&] {
        assert(minirpc::Coroutine::Current() == outer);
        events.push_back(1);
        inner->Resume();
        assert(minirpc::Coroutine::Current() == outer);
        events.push_back(3);
        inner->Resume();
        events.push_back(5);
    });
    outer = &outer_co;
    inner = &inner_co;

    outer->Resume();

    assert((events == std::vector<int>{1, 2, 3, 4, 5}));
    assert(outer->Finished());
    assert(inner->Finished());
    assert(minirpc::Coroutine::Current() == nullptr);
}

void TestEntryExceptionMarksCoroutineFailed() {
    minirpc::Coroutine co([] { throw std::runtime_error("boom"); });
    co.Resume();
    assert(co.Finished());
    assert(co.failed());
    assert(minirpc::Coroutine::Current() == nullptr);
}

void TestFinishedCoroutineReleasesCapturedResources() {
    auto resource = std::make_shared<int>(42);
    std::weak_ptr<int> weak_resource = resource;
    minirpc::Coroutine co([resource] { assert(*resource == 42); });
    resource.reset();

    assert(!weak_resource.expired());
    co.Resume();
    assert(co.Finished());
    assert(weak_resource.expired());
}

}  // namespace

int main() {
    TestResumeYieldSequence();
    TestMultipleCoroutinesKeepSeparateStacks();
    TestNestedResumeReturnsToCallerCoroutine();
    TestEntryExceptionMarksCoroutineFailed();
    TestFinishedCoroutineReleasesCapturedResources();
    return 0;
}
