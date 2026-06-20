#pragma once
#include <vector>

struct QueuePartitionInfo {
	int leader_id;
	unsigned long long leader_offset;
	unsigned long long leader_commited_offset;
	std::vector<int> follower_ids;
	std::vector<unsigned long long> follower_offsets;
};