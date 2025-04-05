#pragma once
#include <memory>
#include <unordered_map>
#include <mutex>
#include "./Partition.h"
#include "./QueueMetadata.h"

class Queue {
private:
	std::shared_ptr<QueueMetadata> metadata;

	std::unordered_map<unsigned int, std::shared_ptr<Partition>> partitions;

	std::mutex mut;
public:
	Queue(std::shared_ptr<QueueMetadata> metadata);

	QueueMetadata* get_metadata();

	void add_partition(std::shared_ptr<Partition> partition);
	Partition* get_partition(unsigned int partition_id);
};