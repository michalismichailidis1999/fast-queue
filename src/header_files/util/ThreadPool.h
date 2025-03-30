#pragma once
#include <queue>
#include <functional>
#include <thread>
#include <mutex>

class ThreadPool {
private:
	std::vector<std::thread> workers;
	std::queue<std::function<void()>> tasks;

	std::mutex mut;
	std::condition_variable condition;
	std::atomic_bool stop;
public:
	ThreadPool(int total_threads);
	~ThreadPool();

	void enqueue(std::function<void()> task);

	void stop_workers();
};