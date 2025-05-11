#pragma once
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <shared_mutex>
#include "./Queue.h"
#include "./messages_management/Producer.h"
#include "./messages_management/Consumer.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../logging/Logger.h"

class QueueManager {
private:
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Logger* logger;

	std::map<std::string, std::shared_ptr<Queue>> queues;

	std::shared_mutex mut;
public:
	QueueManager(FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger);

	void add_queue(std::shared_ptr<Queue> queue);
	bool has_queue(const std::string& queue_name);
	std::shared_ptr<Queue> get_queue(const std::string& queue_name);
	void remove_queue(const std::string& queue_name);

	void create_queue(QueueMetadata* metadata);
	void delete_queue(const std::string& queue_name);

	void get_all_queue_names(std::vector<std::string>* queues_list);
};