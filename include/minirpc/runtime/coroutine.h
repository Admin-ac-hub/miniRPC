#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

namespace minirpc {

constexpr std::size_t kDefaultCoroutineStackSize = 128 * 1024;

struct CoroutineContext {
    std::uintptr_t regs[14]{};
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

    static Coroutine* Current() noexcept;

private:
    static void Trampoline(Coroutine* coroutine) noexcept;
    void Run() noexcept;

    CoroutineContext context_;
    CoroutineContext* caller_context_;
    std::unique_ptr<unsigned char[]> stack_;
    std::size_t stack_size_;
    Entry entry_;
    State state_;
    bool failed_;
};

}  // namespace minirpc
