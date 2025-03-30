#pragma once
#include <string>
#include <mutex>
#include "Enums.h"

class QueueMetadata {
private:
	std::string name;

	unsigned int partitions;
	unsigned int replication_factor;

	Status status;

	std::mutex mut;
public:
	QueueMetadata(const std::string& name, unsigned int partitions = 1, unsigned int replication_factor = 1);

	QueueMetadata(void* metadata);

	const std::string& get_name();
	unsigned int get_partitions();
	unsigned int get_replication_factor();

	void set_status(Status status);
	Status get_status();

	std::tuple<int, std::shared_ptr<char>> get_metadata_bytes();
};