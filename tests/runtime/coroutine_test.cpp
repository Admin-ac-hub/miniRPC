#include <array>
#include <cassert>
#include <cfenv>
#include <cstdint>
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

void TestFloatingPointEnvironmentIsIsolated() {
    const int original_rounding = std::fegetround();
    assert(std::fesetround(FE_DOWNWARD) == 0);

    minirpc::Coroutine* coroutine = nullptr;
    int rounding_before_yield = 0;
    int rounding_after_yield = 0;
    minirpc::Coroutine co([&] {
        rounding_before_yield = std::fegetround();
        assert(std::fesetround(FE_UPWARD) == 0);
        coroutine->Yield();
        rounding_after_yield = std::fegetround();
    });
    coroutine = &co;

    co.Resume();
    assert(rounding_before_yield == FE_DOWNWARD);
    assert(std::fegetround() == FE_DOWNWARD);

    assert(std::fesetround(FE_TOWARDZERO) == 0);
    co.Resume();
    assert(rounding_after_yield == FE_UPWARD);
    assert(std::fegetround() == FE_TOWARDZERO);
    assert(std::fesetround(original_rounding) == 0);
}

#if defined(__aarch64__)
void TestAarch64CalleeSavedSimdRegisterIsIsolated() {
    const std::array<std::uint64_t, 8> main_values{
        0x1000000000000008ULL, 0x1000000000000009ULL,
        0x100000000000000aULL, 0x100000000000000bULL,
        0x100000000000000cULL, 0x100000000000000dULL,
        0x100000000000000eULL, 0x100000000000000fULL,
    };
    const std::array<std::uint64_t, 8> coroutine_values{
        0x2000000000000008ULL, 0x2000000000000009ULL,
        0x200000000000000aULL, 0x200000000000000bULL,
        0x200000000000000cULL, 0x200000000000000dULL,
        0x200000000000000eULL, 0x200000000000000fULL,
    };
    std::array<std::uint64_t, 8> main_after_yield{};
    std::array<std::uint64_t, 8> coroutine_after_resume{};
    minirpc::Coroutine* coroutine = nullptr;

    minirpc::Coroutine co([&] {
        asm volatile(
            "fmov d8, %0\n\t"
            "fmov d9, %1\n\t"
            "fmov d10, %2\n\t"
            "fmov d11, %3\n\t"
            "fmov d12, %4\n\t"
            "fmov d13, %5\n\t"
            "fmov d14, %6\n\t"
            "fmov d15, %7"
            :
            : "r"(coroutine_values[0]), "r"(coroutine_values[1]),
              "r"(coroutine_values[2]), "r"(coroutine_values[3]),
              "r"(coroutine_values[4]), "r"(coroutine_values[5]),
              "r"(coroutine_values[6]), "r"(coroutine_values[7])
            : "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15");
        coroutine->Yield();
        asm volatile(
            "fmov %0, d8\n\t"
            "fmov %1, d9\n\t"
            "fmov %2, d10\n\t"
            "fmov %3, d11\n\t"
            "fmov %4, d12\n\t"
            "fmov %5, d13\n\t"
            "fmov %6, d14\n\t"
            "fmov %7, d15"
            : "=r"(coroutine_after_resume[0]), "=r"(coroutine_after_resume[1]),
              "=r"(coroutine_after_resume[2]), "=r"(coroutine_after_resume[3]),
              "=r"(coroutine_after_resume[4]), "=r"(coroutine_after_resume[5]),
              "=r"(coroutine_after_resume[6]), "=r"(coroutine_after_resume[7]));
    });
    coroutine = &co;

    asm volatile(
        "fmov d8, %0\n\t"
        "fmov d9, %1\n\t"
        "fmov d10, %2\n\t"
        "fmov d11, %3\n\t"
        "fmov d12, %4\n\t"
        "fmov d13, %5\n\t"
        "fmov d14, %6\n\t"
        "fmov d15, %7"
        :
        : "r"(main_values[0]), "r"(main_values[1]),
          "r"(main_values[2]), "r"(main_values[3]),
          "r"(main_values[4]), "r"(main_values[5]),
          "r"(main_values[6]), "r"(main_values[7])
        : "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15");
    co.Resume();
    asm volatile(
        "fmov %0, d8\n\t"
        "fmov %1, d9\n\t"
        "fmov %2, d10\n\t"
        "fmov %3, d11\n\t"
        "fmov %4, d12\n\t"
        "fmov %5, d13\n\t"
        "fmov %6, d14\n\t"
        "fmov %7, d15"
        : "=r"(main_after_yield[0]), "=r"(main_after_yield[1]),
          "=r"(main_after_yield[2]), "=r"(main_after_yield[3]),
          "=r"(main_after_yield[4]), "=r"(main_after_yield[5]),
          "=r"(main_after_yield[6]), "=r"(main_after_yield[7]));
    assert(main_after_yield == main_values);

    co.Resume();
    assert(coroutine_after_resume == coroutine_values);
}
#endif

}  // namespace

int main() {
    TestResumeYieldSequence();
    TestMultipleCoroutinesKeepSeparateStacks();
    TestNestedResumeReturnsToCallerCoroutine();
    TestEntryExceptionMarksCoroutineFailed();
    TestFinishedCoroutineReleasesCapturedResources();
    TestFloatingPointEnvironmentIsIsolated();
#if defined(__aarch64__)
    TestAarch64CalleeSavedSimdRegisterIsIsolated();
#endif
    return 0;
}
