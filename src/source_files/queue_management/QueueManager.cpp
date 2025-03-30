#include "QueueManager.h"
#include "QueueMetadata.h"

QueueManager::QueueManager() {}

void QueueManager::add_queue(std::shared_ptr<Queue> queue) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->queues[queue.get()->get_metadata()->get_name()] = queue;
}

bool QueueManager::has_queue(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->queues.find(queue_name) != this->queues.end();
}

std::shared_ptr<Queue> QueueManager::get_queue(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->mut);

	if (this->queues.find(queue_name) == this->queues.end()) return nullptr;

	return this->queues[queue_name];
}

void QueueManager::remove_queue(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->queues.erase(queue_name);
}

void QueueManager::get_all_queue_names(std::vector<std::string>* queues_list) {
	std::lock_guard<std::mutex> lock(this->mut);

	for (auto iter : this->queues)
		(*queues_list).push_back(iter.first);
}