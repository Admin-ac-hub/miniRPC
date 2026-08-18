#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace minirpc {

constexpr std::size_t kDefaultCoroutineStackSize = 128 * 1024;

struct CoroutineHandle {
    std::uint64_t id = 0;
    std::uint64_t generation = 0;

    explicit operator bool() const noexcept { return id != 0 && generation != 0; }
};

inline bool operator==(CoroutineHandle lhs, CoroutineHandle rhs) noexcept {
    return lhs.id == rhs.id && lhs.generation == rhs.generation;
}

inline bool operator!=(CoroutineHandle lhs, CoroutineHandle rhs) noexcept {
    return !(lhs == rhs);
}

struct CoroutineContext {
    std::uintptr_t regs[14]{};
#if defined(__x86_64__)
    std::uint32_t mxcsr = 0;
    std::uint16_t x87_control = 0;
    std::uint16_t reserved = 0;
#elif defined(__aarch64__)
    std::uint64_t callee_saved_simd[8]{};
    std::uint64_t fpcr = 0;
    std::uint64_t fpsr = 0;
#else
#error "miniRPC coroutine context supports x86_64 and aarch64 Linux only"
#endif
};

class Coroutine {
public:
    using Entry = std::function<void()>;

    enum class State {
        kReady,
        kRunning,
        kSuspended,
        kFinished,
    };

    explicit Coroutine(Entry entry, std::size_t stack_size = kDefaultCoroutineStackSize);
    ~Coroutine();

    Coroutine(const Coroutine&) = delete;
    Coroutine& operator=(const Coroutine&) = delete;

    void Resume() noexcept;
    void Yield() noexcept;

    bool Finished() const noexcept;
    State state() const noexcept;
    bool failed() const noexcept;
    CoroutineHandle handle() const noexcept;

    static Coroutine* Current() noexcept;

private:
    friend class Scheduler;

    static void Trampoline(Coroutine* coroutine) noexcept;
    void Run() noexcept;
    void SetHandle(CoroutineHandle handle) noexcept;
    void AllocateStack(std::size_t requested_size);
    void ReleaseStack() noexcept;

    CoroutineContext context_;
    CoroutineContext* caller_context_;
    Coroutine* caller_coroutine_;
    unsigned char* stack_;
    std::size_t stack_size_;
    void* stack_mapping_;
    std::size_t stack_mapping_size_;
    Entry entry_;
    State state_;
    bool failed_;
    CoroutineHandle handle_;
    void* sanitizer_fake_stack_;
};

}  // namespace minirpc
