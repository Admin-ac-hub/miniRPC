#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "minirpc/runtime/thread_pool.h"

namespace {

void TestStopDrainsQueueAndRestartDoesNotReplayTasks() {
    minirpc::ThreadPool pool(1, 2);
    assert(!pool.Post([] {}));
    pool.Stop();
    int executed = 0;
    for (int run = 0; run < 3; ++run) {
        pool.Start();
        pool.Start();
        std::promise<void> entered;
        std::promise<void> release;
        auto gate = release.get_future().share();
        assert(pool.Post([&entered, gate] {
            entered.set_value();
            gate.wait();
        }));
        assert(entered.get_future().wait_for(std::chrono::seconds(1)) ==
               std::future_status::ready);
        auto resource = std::make_shared<int>(42);
        std::weak_ptr<int> weak = resource;
        assert(pool.Post([resource, &executed] {
            assert(*resource == 42);
            ++executed;
        }));
        assert(pool.Post([&executed] { ++executed; }));
        resource.reset();
        assert(pool.queued_tasks() == 2);
        assert(!pool.Post([] {}));
        auto stopped = std::async(std::launch::async, [&] { pool.Stop(); });
        assert(stopped.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
        release.set_value();
        assert(stopped.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
        stopped.get();
        assert(executed == (run + 1) * 2);
        assert(pool.queued_tasks() == 0);
        assert(weak.expired());
        assert(!pool.Post([] {}));
        pool.Stop();
    }
}

void TestStopAccountsForConcurrentPosts() {
    minirpc::ThreadPool pool(2, 64);
    pool.Start();
    std::atomic<int> accepted{0};
    std::atomic<int> executed{0};
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::vector<std::thread> producers;
    for (int i = 0; i < 4; ++i) {
        producers.emplace_back([&] {
            gate.wait();
            for (int j = 0; j < 1000; ++j) {
                if (pool.Post([&] { executed.fetch_add(1); })) accepted.fetch_add(1);
            }
        });
    }
    release.set_value();
    pool.Stop();
    for (auto& producer : producers) producer.join();
    assert(executed.load() == accepted.load());
    assert(pool.queued_tasks() == 0);
}

}  // namespace

int main() {
    TestStopDrainsQueueAndRestartDoesNotReplayTasks();
    TestStopAccountsForConcurrentPosts();
}
