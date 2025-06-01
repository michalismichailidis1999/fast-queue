#pragma once
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include "./Partition.h"
#include "./QueueMetadata.h"

class Queue {
private:
	std::shared_ptr<QueueMetadata> metadata;

	std::unordered_map<unsigned int, std::shared_ptr<Partition>> partitions;

	std::shared_mutex mut;
public:
	Queue(std::shared_ptr<QueueMetadata> metadata);

	QueueMetadata* get_metadata();

	void add_partition(std::shared_ptr<Partition> partition);
	void remove_partition(unsigned int partition_id);
	Partition* get_partition(unsigned int partition_id);
};