#pragma once
#include <shared_mutex>
#include <unordered_map>
#include <tuple>
#include "./Partition.h"
#include "./PartitionSegment.h"

class SegmentLockManager {
private:
	std::unordered_map<std::string, std::tuple<std::shared_mutex, unsigned int>> locks;
	std::mutex mut;

	const std::string& get_segment_key(Partition* partition, PartitionSegment* segment);
public:
	SegmentLockManager();

	void lock_segment(Partition* partition, PartitionSegment* segment);
	void release_segment_lock(Partition* partition, PartitionSegment* segment);
};