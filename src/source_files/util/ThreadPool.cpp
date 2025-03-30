#include "../../header_files/util/ThreadPool.h"

ThreadPool::ThreadPool(int total_threads) {
    this->stop = false;

    for (int i = 0; i < total_threads; ++i) {
        workers.emplace_back([&] {
            while (true) {
                std::function<void()> task;

                {
                    std::unique_lock<std::mutex> lock(mut);

                    condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });

                    if (stop) return;

                    task = std::move(tasks.front());
                    tasks.pop();
                }

                task();
            }
        });
    }
}

void ThreadPool::enqueue(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(mut);

        if (stop) {
            printf("Cannot enqueue task. Application is stopping...\n");
            return;
        }

        tasks.emplace(task);
    }

    condition.notify_one();
}

void ThreadPool::stop_workers() {
    bool expected = false;

    if (!this->stop.compare_exchange_weak(expected, true)) return;

    condition.notify_all();
    printf("Terminating workers...\n");
}

ThreadPool::~ThreadPool() {
    this->stop_workers();

    for (std::thread& worker : workers)
        worker.join();
}