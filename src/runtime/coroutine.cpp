#include "minirpc/runtime/coroutine.h"

#include <algorithm>
#include <utility>

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

extern "C" void minirpc_coctx_swap(CoroutineContext* from, CoroutineContext* to);

std::uintptr_t AlignStackTop(unsigned char* stack, std::size_t stack_size) noexcept {
    std::uintptr_t top = reinterpret_cast<std::uintptr_t>(stack + stack_size);
    top &= ~static_cast<std::uintptr_t>(0xF);
#if defined(__x86_64__)
    return top - 8;
#else
    return top;
#endif
}

}  // namespace

Coroutine::Coroutine(Entry entry, std::size_t stack_size)
    : caller_context_(nullptr),
      stack_(new unsigned char[std::max(stack_size, static_cast<std::size_t>(16 * 1024))]),
      stack_size_(std::max(stack_size, static_cast<std::size_t>(16 * 1024))),
      entry_(std::move(entry)),
      state_(State::kReady),
      failed_(false) {
    context_.regs[kRSP] = AlignStackTop(stack_.get(), stack_size_);
    context_.regs[kRETAddr] = reinterpret_cast<std::uintptr_t>(&Coroutine::Trampoline);
    context_.regs[kRDI] = reinterpret_cast<std::uintptr_t>(this);
}

Coroutine::~Coroutine() = default;

void Coroutine::Resume() noexcept {
    if (state_ == State::kFinished || state_ == State::kRunning) {
        return;
    }

    Coroutine* previous = t_current_coroutine;
    caller_context_ = previous == nullptr ? &t_main_context : &previous->context_;
    state_ = State::kRunning;
    t_current_coroutine = this;
    minirpc_coctx_swap(caller_context_, &context_);
    t_current_coroutine = previous;
    if (Finished()) {
        entry_ = nullptr;
        stack_.reset();
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
    minirpc_coctx_swap(&context_, caller_context_);
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

Coroutine* Coroutine::Current() noexcept {
    return t_current_coroutine;
}

void Coroutine::Trampoline(Coroutine* coroutine) noexcept {
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
