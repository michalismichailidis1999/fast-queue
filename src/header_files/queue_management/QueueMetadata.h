#pragma once
#include <string>
#include <shared_mutex>
#include "../Constants.h"
#include "../Enums.h"
#include "../util/Helper.h"

class QueueMetadata {
private:
	std::string name;

	unsigned int partitions;
	unsigned int replication_factor;

	CleanupPolicyType cleanup_policy;

	// for __cluster_metadata queue only
	unsigned long long last_commit_index;
	unsigned long long last_applied_index;

	Status status;

	std::shared_mutex mut;
public:
	QueueMetadata(const std::string& name, unsigned int partitions = 1, unsigned int replication_factor = 1, CleanupPolicyType cleanup_policy = CleanupPolicyType::DELETE_SEGMENTS);

	QueueMetadata(void* metadata);

	const std::string& get_name();
	unsigned int get_partitions();
	unsigned int get_replication_factor();

	unsigned long long get_last_commit_index();
	unsigned long long get_last_applied_index();

	void set_status(Status status);
	Status get_status();

	CleanupPolicyType get_cleanup_policy();

	std::tuple<int, std::shared_ptr<char>> get_metadata_bytes();
};