#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/net/socket_utils.h"
#include "minirpc/runtime/coroutine_io_context.h"

namespace {

using namespace std::chrono_literals;

class SocketPair {
public:
    SocketPair() {
        const int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds_);
        assert(rc == 0);
        std::string error;
        assert(minirpc::SetNonBlocking(fds_[0], &error));
        assert(minirpc::SetNonBlocking(fds_[1], &error));
    }

    ~SocketPair() {
        if (fds_[0] != -1) {
            ::close(fds_[0]);
        }
        if (fds_[1] != -1) {
            ::close(fds_[1]);
        }
    }

    int first() const noexcept { return fds_[0]; }
    int second() const noexcept { return fds_[1]; }

private:
    int fds_[2]{-1, -1};
};

void TestReadSuspendsUntilFdReadable() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    std::vector<int> events;
    std::mutex events_mutex;
    auto push_event = [&](int event) {
        std::lock_guard<std::mutex> lock(events_mutex);
        events.push_back(event);
    };
    char buffer[16]{};

    io.Spawn([&] {
        push_event(1);
        const ssize_t n = io.Read(sockets.first(), buffer, sizeof(buffer));
        assert(n == 5);
        assert(std::string(buffer, static_cast<std::size_t>(n)) == "hello");
        push_event(3);
    });

    std::thread writer([&] {
        std::this_thread::sleep_for(20ms);
        push_event(2);
        const ssize_t n = ::write(sockets.second(), "hello", 5);
        assert(n == 5);
    });

    io.Run();
    writer.join();

    assert((events == std::vector<int>{1, 2, 3}));
    assert(io.waiting_count() == 0);
}

void TestWriteAllSuspendsUntilFdWritable() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    std::string payload(256 * 1024, 'x');
    std::string received;
    std::vector<int> events;
    std::mutex events_mutex;
    auto push_event = [&](int event) {
        std::lock_guard<std::mutex> lock(events_mutex);
        events.push_back(event);
    };

    int send_buffer = 4096;
    (void)::setsockopt(sockets.first(), SOL_SOCKET, SO_SNDBUF, &send_buffer, sizeof(send_buffer));

    std::thread reader([&] {
        std::this_thread::sleep_for(20ms);
        push_event(2);
        char buffer[8192];
        while (received.size() < payload.size()) {
            const ssize_t n = ::read(sockets.second(), buffer, sizeof(buffer));
            if (n > 0) {
                received.append(buffer, static_cast<std::size_t>(n));
                continue;
            }
            if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                std::this_thread::sleep_for(1ms);
                continue;
            }
            break;
        }
    });

    io.Spawn([&] {
        push_event(1);
        const ssize_t n = io.WriteAll(sockets.first(), payload.data(), payload.size());
        assert(n == static_cast<ssize_t>(payload.size()));
        push_event(3);
    });

    io.Run();
    reader.join();

    assert(received == payload);
    assert((events == std::vector<int>{1, 2, 3}));
    assert(io.waiting_count() == 0);
}

void TestTimerAndIoShareEventLoop() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    std::vector<int> events;
    std::mutex events_mutex;
    auto push_event = [&](int event) {
        std::lock_guard<std::mutex> lock(events_mutex);
        events.push_back(event);
    };
    char buffer[8]{};

    io.Spawn([&] {
        push_event(1);
        assert(io.timers().SleepFor(10ms));
        push_event(2);
    });
    io.Spawn([&] {
        const ssize_t n = io.Read(sockets.first(), buffer, sizeof(buffer));
        assert(n == 4);
        push_event(4);
    });

    std::thread writer([&] {
        std::this_thread::sleep_for(30ms);
        push_event(3);
        const ssize_t n = ::write(sockets.second(), "done", 4);
        assert(n == 4);
    });

    io.Run();
    writer.join();

    assert((events == std::vector<int>{1, 2, 3, 4}));
}

void TestStopCancelsReadableWait() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    ssize_t read_result = 0;
    int read_error = 0;

    io.Spawn([&] {
        char byte = 0;
        read_result = io.Read(sockets.first(), &byte, sizeof(byte));
        read_error = errno;
    });
    io.Spawn([&] { io.Stop(); });

    io.Run();

    assert(read_result == -1);
    assert(read_error == ECANCELED);
    assert(io.waiting_count() == 0);
    assert(io.scheduler().coroutine_count() == 0);
}

void TestStopCancelsTimerWait() {
    minirpc::CoroutineIoContext io;
    bool sleep_result = true;

    io.Spawn([&] {
        sleep_result = io.timers().SleepFor(std::chrono::hours(1));
    });
    io.Spawn([&] { io.Stop(); });

    io.Run();

    assert(!sleep_result);
    assert(io.timers().empty());
    assert(io.scheduler().coroutine_count() == 0);
}

void TestExternalStopWakesAndCancelsReadableWait() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    std::promise<void> waiter_registered;
    auto waiter_ready = waiter_registered.get_future();
    ssize_t read_result = 0;
    int read_error = 0;

    io.Spawn([&] {
        char byte = 0;
        read_result = io.Read(sockets.first(), &byte, sizeof(byte));
        read_error = errno;
    });
    io.Spawn([&] { waiter_registered.set_value(); });

    std::thread io_thread([&] { io.Run(); });
    assert(waiter_ready.wait_for(1s) == std::future_status::ready);
    io.Stop();
    io_thread.join();

    assert(read_result == -1);
    assert(read_error == ECANCELED);
    assert(io.waiting_count() == 0);
    assert(io.scheduler().coroutine_count() == 0);
}

void TestPostedControlRunsOnIoThread() {
    minirpc::CoroutineIoContext io;
    bool executed = false;

    assert(io.Post([&] {
        assert(minirpc::CoroutineIoContext::Current() == &io);
        executed = true;
    }));
    io.Run();

    assert(executed);
}

void TestConcurrentControlPostingDoesNotLoseWork() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    std::promise<void> waiter_registered;
    auto waiter_ready = waiter_registered.get_future();
    std::atomic<int> controls_run{0};

    io.Spawn([&] {
        char byte = 0;
        (void)io.Read(sockets.first(), &byte, sizeof(byte));
    });
    io.Spawn([&] { waiter_registered.set_value(); });

    std::thread io_thread([&] { io.Run(); });
    assert(waiter_ready.wait_for(1s) == std::future_status::ready);

    constexpr int kPosterCount = 4;
    constexpr int kControlsPerPoster = 250;
    std::vector<std::thread> posters;
    for (int i = 0; i < kPosterCount; ++i) {
        posters.emplace_back([&] {
            for (int j = 0; j < kControlsPerPoster; ++j) {
                assert(io.Post([&] { controls_run.fetch_add(1); }));
            }
        });
    }
    for (auto& poster : posters) {
        poster.join();
    }

    io.Stop();
    io_thread.join();

    assert(controls_run.load() == kPosterCount * kControlsPerPoster);
    assert(io.waiting_count() == 0);
    assert(io.scheduler().coroutine_count() == 0);
}

}  // namespace

int main() {
    TestReadSuspendsUntilFdReadable();
    TestWriteAllSuspendsUntilFdWritable();
    TestTimerAndIoShareEventLoop();
    TestStopCancelsReadableWait();
    TestStopCancelsTimerWait();
    TestExternalStopWakesAndCancelsReadableWait();
    TestPostedControlRunsOnIoThread();
    TestConcurrentControlPostingDoesNotLoseWork();
    return 0;
}
