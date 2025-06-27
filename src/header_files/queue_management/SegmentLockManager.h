#pragma once
#include <shared_mutex>
#include <unordered_map>
#include <tuple>
#include <future>
#include "./Partition.h"
#include "./PartitionSegment.h"

typedef struct {
	std::shared_mutex mut;
	int references;
} segment_lock;

class SegmentLockManager {
private:
	std::unordered_map<std::string, std::shared_ptr<segment_lock>> locks;
	std::mutex mut;

	std::string get_segment_key(Partition* partition, PartitionSegment* segment);
public:
	SegmentLockManager();

	void lock_segment(Partition* partition, PartitionSegment* segment, bool exclusive = false);
	void release_segment_lock(Partition* partition, PartitionSegment* segment, bool exclusive = false);
};