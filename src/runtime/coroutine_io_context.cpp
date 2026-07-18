#include "minirpc/runtime/coroutine_io_context.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <limits>
#include <utility>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace minirpc {
namespace {

constexpr int kMaxEvents = 64;
thread_local CoroutineIoContext* t_current_io_context = nullptr;

int ClampTimeoutMs(TimerQueue::Duration duration) {
    using namespace std::chrono;
    if (duration <= TimerQueue::Duration::zero()) {
        return 0;
    }
    const auto ms = duration_cast<milliseconds>(duration);
    const auto rounded = ms.count() == 0 ? 1 : ms.count();
    return static_cast<int>(std::min<long long>(rounded, std::numeric_limits<int>::max()));
}

}  // namespace

CoroutineIoContext::CoroutineIoContext(std::size_t default_stack_size)
    : scheduler_(default_stack_size),
      timers_(&scheduler_),
      epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),
      wake_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      stopping_(false),
      external_waits_(0) {
    if (epoll_fd_ != -1 && wake_fd_ != -1) {
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = wake_fd_;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &event) == -1) {
            ::close(wake_fd_);
            wake_fd_ = -1;
        }
    }
}

CoroutineIoContext::~CoroutineIoContext() {
    if (wake_fd_ != -1) {
        ::close(wake_fd_);
        wake_fd_ = -1;
    }
    if (epoll_fd_ != -1) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

Coroutine* CoroutineIoContext::Spawn(Task task) {
    return scheduler_.Spawn(std::move(task));
}

void CoroutineIoContext::Schedule(Coroutine* coroutine) {
    if (coroutine == nullptr || coroutine->Finished()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_ready_.push_back(coroutine);
    }
    Wake();
}

void CoroutineIoContext::AddExternalWait() noexcept {
    external_waits_.fetch_add(1, std::memory_order_acq_rel);
}

void CoroutineIoContext::CompleteExternalWait() noexcept {
    std::size_t current = external_waits_.load(std::memory_order_acquire);
    while (current != 0 &&
           !external_waits_.compare_exchange_weak(
               current, current - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
}

void CoroutineIoContext::Run() {
    CoroutineIoContext* previous = t_current_io_context;
    t_current_io_context = this;

    epoll_event events[kMaxEvents];
    while (!stopping_.load(std::memory_order_acquire) && HasPendingWork()) {
        DrainPendingReady();
        scheduler_.Run();
        timers_.DrainExpired();
        DrainPendingReady();
        if (scheduler_.ready_count() > 0) {
            continue;
        }
        if (!HasPendingWork()) {
            break;
        }

        const int timeout_ms = NextPollTimeoutMs();
        const int ready = ::epoll_wait(epoll_fd_, events, kMaxEvents, timeout_ms);
        if (ready == -1) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        for (int i = 0; i < ready; ++i) {
            const int fd = static_cast<int>(events[i].data.fd);
            if (fd == wake_fd_) {
                DrainWake();
            } else {
                WakeFd(fd, events[i].events);
            }
        }
        timers_.DrainExpired();
        DrainPendingReady();
    }

    t_current_io_context = previous;
}

void CoroutineIoContext::Stop() noexcept {
    stopping_.store(true, std::memory_order_release);
    Wake();
}

bool CoroutineIoContext::WaitReadable(int fd) {
    return WaitFd(fd, WaitKind::kRead);
}

bool CoroutineIoContext::WaitWritable(int fd) {
    return WaitFd(fd, WaitKind::kWrite);
}

ssize_t CoroutineIoContext::Read(int fd, void* buffer, std::size_t size) {
    while (true) {
        const ssize_t n = ::read(fd, buffer, size);
        if (n >= 0) {
            return n;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (!WaitReadable(fd)) {
                errno = EINVAL;
                return -1;
            }
            continue;
        }
        return -1;
    }
}

ssize_t CoroutineIoContext::WriteAll(int fd, const void* buffer, std::size_t size) {
    const char* data = static_cast<const char*>(buffer);
    std::size_t written = 0;
    while (written < size) {
        const ssize_t n = ::send(fd, data + written, size - written, MSG_NOSIGNAL);
        if (n > 0) {
            written += static_cast<std::size_t>(n);
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!WaitWritable(fd)) {
                errno = EINVAL;
                return -1;
            }
            continue;
        }
        return -1;
    }
    return static_cast<ssize_t>(written);
}

Scheduler& CoroutineIoContext::scheduler() noexcept {
    return scheduler_;
}

TimerQueue& CoroutineIoContext::timers() noexcept {
    return timers_;
}

std::size_t CoroutineIoContext::waiting_count() const noexcept {
    std::size_t count = 0;
    for (const auto& [_, waiters] : waiters_) {
        if (waiters.read != nullptr) {
            ++count;
        }
        if (waiters.write != nullptr) {
            ++count;
        }
    }
    return count;
}

CoroutineIoContext* CoroutineIoContext::Current() noexcept {
    return t_current_io_context;
}

bool CoroutineIoContext::WaitFd(int fd, WaitKind kind) {
    if (fd < 0 || epoll_fd_ == -1 || Coroutine::Current() == nullptr) {
        return false;
    }
    FdWaiters& waiters = waiters_[fd];
    Coroutine*& slot = kind == WaitKind::kRead ? waiters.read : waiters.write;
    if (slot != nullptr) {
        return false;
    }
    slot = Coroutine::Current();
    if (!UpdateInterest(fd, &waiters)) {
        slot = nullptr;
        UpdateInterest(fd, &waiters);
        return false;
    }
    Scheduler::SuspendCurrent();
    return true;
}

void CoroutineIoContext::WakeFd(int fd, std::uint32_t events) {
    auto it = waiters_.find(fd);
    if (it == waiters_.end()) {
        return;
    }

    FdWaiters& waiters = it->second;
    const bool wake_read = (events & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0;
    const bool wake_write = (events & (EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0;

    Coroutine* read = nullptr;
    Coroutine* write = nullptr;
    if (wake_read) {
        read = waiters.read;
        waiters.read = nullptr;
    }
    if (wake_write) {
        write = waiters.write;
        waiters.write = nullptr;
    }

    (void)UpdateInterest(fd, &waiters);
    if (read != nullptr) {
        scheduler_.Schedule(read);
    }
    if (write != nullptr && write != read) {
        scheduler_.Schedule(write);
    }
}

bool CoroutineIoContext::UpdateInterest(int fd, FdWaiters* waiters) {
    if (waiters == nullptr) {
        return false;
    }

    std::uint32_t events = EPOLLRDHUP;
    if (waiters->read != nullptr) {
        events |= EPOLLIN;
    }
    if (waiters->write != nullptr) {
        events |= EPOLLOUT;
    }

    if ((events & (EPOLLIN | EPOLLOUT)) == 0) {
        if (waiters->registered) {
            (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
        waiters_.erase(fd);
        return true;
    }

    epoll_event event{};
    event.events = events;
    event.data.fd = fd;
    const int op = waiters->registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    if (::epoll_ctl(epoll_fd_, op, fd, &event) == -1) {
        return false;
    }
    waiters->registered = true;
    return true;
}

void CoroutineIoContext::DrainWake() {
    if (wake_fd_ == -1) {
        return;
    }
    uint64_t value = 0;
    while (true) {
        const ssize_t n = ::read(wake_fd_, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        return;
    }
}

void CoroutineIoContext::DrainPendingReady() {
    std::vector<Coroutine*> pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending.swap(pending_ready_);
    }
    for (Coroutine* coroutine : pending) {
        scheduler_.Schedule(coroutine);
    }
}

void CoroutineIoContext::Wake() {
    if (wake_fd_ == -1) {
        return;
    }
    const uint64_t value = 1;
    while (true) {
        const ssize_t n = ::write(wake_fd_, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            return;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        return;
    }
}

bool CoroutineIoContext::HasPendingWork() const {
    bool has_pending_ready = false;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        has_pending_ready = !pending_ready_.empty();
    }
    return scheduler_.ready_count() > 0 || has_pending_ready || !timers_.empty() ||
           !waiters_.empty() || external_waits_.load(std::memory_order_acquire) > 0;
}

int CoroutineIoContext::NextPollTimeoutMs() const {
    auto timeout = timers_.TimeUntilNext();
    if (!timeout.has_value()) {
        return -1;
    }
    return ClampTimeoutMs(*timeout);
}

}  // namespace minirpc
