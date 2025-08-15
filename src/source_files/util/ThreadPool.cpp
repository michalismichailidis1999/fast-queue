#include "../../header_files/util/ThreadPool.h"

ThreadPool::ThreadPool(int total_threads) {
    this->stop = false;

    for (int i = 0; i < total_threads; ++i) {
        this->workers.emplace_back([&] {
            while (true) {
                std::function<void()> task;

                {
                    std::unique_lock<std::mutex> lock(mut);

                    this->condition.wait(lock, [this] { return this->stop.load() || !this->tasks.empty(); });

                    if (this->stop.load()) return;

                    task = std::move(this->tasks.front());
                    this->tasks.pop();
                }

                task();
            }
        });
    }
}

void ThreadPool::enqueue(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(mut);

        if (this->stop.load()) {
            printf("Cannot enqueue task. Application is stopping...\n");
            return;
        }

        this->tasks.emplace(task);
    }

    this->condition.notify_one();
}

void ThreadPool::stop_workers() {
    bool expected = false;

    if (!this->stop.compare_exchange_weak(expected, true)) return;

    this->condition.notify_all();
    printf("Terminating workers...\n");
}

ThreadPool::~ThreadPool() {
    this->stop_workers();

    for (std::thread& worker : this->workers)
        worker.join();
}