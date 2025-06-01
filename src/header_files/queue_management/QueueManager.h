#pragma once
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <shared_mutex>
#include "./Queue.h"
#include "./messages_management/Producer.h"
#include "./messages_management/Consumer.h"
#include "./messages_management/index_management/BTreeNode.h"
#include "./messages_management/index_management/SegmentMessageMap.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../logging/Logger.h"

class QueueManager {
private:
	SegmentMessageMap* smm;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Logger* logger;

	std::map<std::string, std::shared_ptr<Queue>> queues;

	std::shared_mutex mut;
public:
	QueueManager(SegmentMessageMap* smm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger);

	void add_queue(std::shared_ptr<Queue> queue);
	bool has_queue(const std::string& queue_name);
	std::shared_ptr<Queue> get_queue(const std::string& queue_name);
	void remove_queue(const std::string& queue_name);

	void create_queue(QueueMetadata* metadata);
	void delete_queue(const std::string& queue_name);

	void add_assigned_partition_to_queue(const std::string& queue_name, unsigned int partition_id);
	void remove_assigned_partition_from_queue(const std::string& queue_name, unsigned int partition_id);

	void get_all_queue_names(std::vector<std::string>* queues_list);
};