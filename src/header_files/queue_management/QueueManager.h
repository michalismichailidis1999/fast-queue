#pragma once
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <mutex>
#include "./Queue.h"
#include "./messages_management/Producer.h"
#include "./messages_management/Consumer.h"

class QueueMetadata;

class QueueManager {
private:
	std::map<std::string, std::shared_ptr<Queue>> queues;

	std::mutex mut;
public:
	QueueManager();

	void add_queue(std::shared_ptr<Queue> queue);
	bool has_queue(const std::string& queue_name);
	std::shared_ptr<Queue> get_queue(const std::string& queue_name);
	void remove_queue(const std::string& queue_name);

	void get_all_queue_names(std::vector<std::string>* queues_list);
};