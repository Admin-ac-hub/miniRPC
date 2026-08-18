#include "minirpc/runtime/coroutine.h"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <new>
#include <utility>

#include <sys/mman.h>
#include <unistd.h>

#if defined(__SANITIZE_ADDRESS__)
#define MINIRPC_HAS_ASAN_FIBER 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define MINIRPC_HAS_ASAN_FIBER 1
#endif
#endif

#ifndef MINIRPC_HAS_ASAN_FIBER
#define MINIRPC_HAS_ASAN_FIBER 0
#endif

namespace minirpc {
namespace {

enum RegisterIndex {
#if defined(__x86_64__)
    kR15 = 0,
    kR14 = 1,
    kR13 = 2,
    kR12 = 3,
    kR9 = 4,
    kR8 = 5,
    kRBP = 6,
    kRDI = 7,
    kRSI = 8,
    kRETAddr = 9,
    kRDX = 10,
    kRCX = 11,
    kRBX = 12,
    kRSP = 13,
#elif defined(__aarch64__)
    kX19 = 0,
    kX20 = 1,
    kX21 = 2,
    kX22 = 3,
    kX23 = 4,
    kX24 = 5,
    kX25 = 6,
    kX26 = 7,
    kX27 = 8,
    kX28 = 9,
    kX29 = 10,
    kRETAddr = 11,
    kRSP = 12,
    kRDI = 13,
#else
#error "miniRPC coroutine context switch supports x86_64 and aarch64 Linux only"
#endif
};

thread_local CoroutineContext t_main_context;
thread_local Coroutine* t_current_coroutine = nullptr;
#if MINIRPC_HAS_ASAN_FIBER
thread_local void* t_main_sanitizer_fake_stack = nullptr;
thread_local void* t_incoming_sanitizer_fake_stack = nullptr;
#endif

extern "C" void minirpc_coctx_swap(CoroutineContext* from, CoroutineContext* to);
extern "C" void minirpc_coctx_init(CoroutineContext* context);
#if MINIRPC_HAS_ASAN_FIBER
extern "C" void __sanitizer_start_switch_fiber(void** fake_stack_save,
                                                const void* stack_bottom,
                                                std::size_t stack_size);
extern "C" void __sanitizer_finish_switch_fiber(void* fake_stack_save,
                                                 const void** old_stack_bottom,
                                                 std::size_t* old_stack_size);
#endif

static_assert(offsetof(CoroutineContext, regs) == 0, "assembly context layout mismatch");
#if defined(__x86_64__)
static_assert(offsetof(CoroutineContext, mxcsr) == 112, "assembly context layout mismatch");
static_assert(offsetof(CoroutineContext, x87_control) == 116,
              "assembly context layout mismatch");
#elif defined(__aarch64__)
static_assert(offsetof(CoroutineContext, callee_saved_simd) == 112,
              "assembly context layout mismatch");
static_assert(offsetof(CoroutineContext, fpcr) == 176, "assembly context layout mismatch");
static_assert(offsetof(CoroutineContext, fpsr) == 184, "assembly context layout mismatch");
#endif

std::uintptr_t AlignStackTop(unsigned char* stack, std::size_t stack_size) noexcept {
    std::uintptr_t top = reinterpret_cast<std::uintptr_t>(stack + stack_size);
    top &= ~static_cast<std::uintptr_t>(0xF);
#if defined(__x86_64__)
    return top - 8;
#else
    return top;
#endif
}

#if MINIRPC_HAS_ASAN_FIBER
void FinishSanitizerSwitch() noexcept {
    void* fake_stack = t_incoming_sanitizer_fake_stack;
    t_incoming_sanitizer_fake_stack = nullptr;
    __sanitizer_finish_switch_fiber(fake_stack, nullptr, nullptr);
}
#endif

}  // namespace

Coroutine::Coroutine(Entry entry, std::size_t stack_size)
    : caller_context_(nullptr),
      caller_coroutine_(nullptr),
      stack_(nullptr),
      stack_size_(0),
      stack_mapping_(nullptr),
      stack_mapping_size_(0),
      entry_(std::move(entry)),
      state_(State::kReady),
      failed_(false),
      sanitizer_fake_stack_(nullptr) {
    AllocateStack(stack_size);
    minirpc_coctx_init(&context_);
    context_.regs[kRSP] = AlignStackTop(stack_, stack_size_);
    context_.regs[kRETAddr] = reinterpret_cast<std::uintptr_t>(&Coroutine::Trampoline);
    context_.regs[kRDI] = reinterpret_cast<std::uintptr_t>(this);
}

Coroutine::~Coroutine() {
    ReleaseStack();
}

void Coroutine::Resume() noexcept {
    if (state_ == State::kFinished || state_ == State::kRunning) {
        return;
    }

    Coroutine* previous = t_current_coroutine;
    caller_coroutine_ = previous;
    caller_context_ = previous == nullptr ? &t_main_context : &previous->context_;
    state_ = State::kRunning;
    t_current_coroutine = this;
#if MINIRPC_HAS_ASAN_FIBER
    void** fake_stack = previous == nullptr
                            ? &t_main_sanitizer_fake_stack
                            : &previous->sanitizer_fake_stack_;
    __sanitizer_start_switch_fiber(fake_stack, stack_, stack_size_);
    t_incoming_sanitizer_fake_stack = *fake_stack;
#endif
    minirpc_coctx_swap(caller_context_, &context_);
#if MINIRPC_HAS_ASAN_FIBER
    FinishSanitizerSwitch();
#endif
    t_current_coroutine = previous;
    caller_coroutine_ = nullptr;
    if (Finished()) {
        entry_ = nullptr;
        ReleaseStack();
    }
}

void Coroutine::Yield() noexcept {
    if (t_current_coroutine != this || caller_context_ == nullptr) {
        return;
    }
    if (state_ == State::kRunning) {
        state_ = State::kSuspended;
    }

    t_current_coroutine = nullptr;
#if MINIRPC_HAS_ASAN_FIBER
    const void* target_stack = caller_coroutine_ == nullptr
                                   ? nullptr
                                   : caller_coroutine_->stack_;
    const std::size_t target_stack_size = caller_coroutine_ == nullptr
                                              ? 0
                                              : caller_coroutine_->stack_size_;
    __sanitizer_start_switch_fiber(
        &sanitizer_fake_stack_, target_stack, target_stack_size);
    t_incoming_sanitizer_fake_stack = sanitizer_fake_stack_;
#endif
    minirpc_coctx_swap(&context_, caller_context_);
#if MINIRPC_HAS_ASAN_FIBER
    FinishSanitizerSwitch();
#endif
    t_current_coroutine = this;

    if (state_ != State::kFinished) {
        state_ = State::kRunning;
    }
}

bool Coroutine::Finished() const noexcept {
    return state_ == State::kFinished;
}

Coroutine::State Coroutine::state() const noexcept {
    return state_;
}

bool Coroutine::failed() const noexcept {
    return failed_;
}

CoroutineHandle Coroutine::handle() const noexcept {
    return handle_;
}

Coroutine* Coroutine::Current() noexcept {
    return t_current_coroutine;
}

void Coroutine::SetHandle(CoroutineHandle handle) noexcept {
    handle_ = handle;
}

void Coroutine::AllocateStack(std::size_t requested_size) {
    const long system_page_size = ::sysconf(_SC_PAGESIZE);
    const std::size_t page_size = system_page_size > 0
                                      ? static_cast<std::size_t>(system_page_size)
                                      : static_cast<std::size_t>(4096);
    const std::size_t minimum_size = 16 * 1024;
    const std::size_t desired_size = std::max(requested_size, minimum_size);
    if (desired_size > std::numeric_limits<std::size_t>::max() - (page_size - 1)) {
        throw std::bad_alloc();
    }
    const std::size_t usable_size = (desired_size + page_size - 1) / page_size * page_size;
    if (usable_size > std::numeric_limits<std::size_t>::max() - 2 * page_size) {
        throw std::bad_alloc();
    }
    const std::size_t mapping_size = usable_size + 2 * page_size;

    void* mapping = ::mmap(
        nullptr, mapping_size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapping == MAP_FAILED) {
        throw std::bad_alloc();
    }
    unsigned char* const stack = static_cast<unsigned char*>(mapping) + page_size;
    if (::mprotect(stack, usable_size, PROT_READ | PROT_WRITE) == -1) {
        (void)::munmap(mapping, mapping_size);
        throw std::bad_alloc();
    }

    stack_ = stack;
    stack_size_ = usable_size;
    stack_mapping_ = mapping;
    stack_mapping_size_ = mapping_size;
}

void Coroutine::ReleaseStack() noexcept {
    if (stack_mapping_ != nullptr) {
        (void)::munmap(stack_mapping_, stack_mapping_size_);
    }
    stack_ = nullptr;
    stack_size_ = 0;
    stack_mapping_ = nullptr;
    stack_mapping_size_ = 0;
}

void Coroutine::Trampoline(Coroutine* coroutine) noexcept {
#if MINIRPC_HAS_ASAN_FIBER
    FinishSanitizerSwitch();
#endif
    if (coroutine != nullptr) {
        coroutine->Run();
        coroutine->Yield();
    }
    while (true) {
    }
}

void Coroutine::Run() noexcept {
    try {
        if (entry_) {
            entry_();
        }
    } catch (...) {
        failed_ = true;
    }
    state_ = State::kFinished;
}

}  // namespace minirpc
