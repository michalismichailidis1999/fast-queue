#include "../../header_files/queue_management/SegmentLockManager.h"

SegmentLockManager::SegmentLockManager() {}

void SegmentLockManager::lock_segment(Partition* partition, PartitionSegment* segment) {
	std::lock_guard<std::mutex> map_lock(this->mut);

	std::string key = this->get_segment_key(partition, segment);
}

void SegmentLockManager::release_segment_lock(Partition* partition, PartitionSegment* segment) {
	std::lock_guard<std::mutex> map_lock(this->mut);

	std::string key = this->get_segment_key(partition, segment);
}

const std::string& SegmentLockManager::get_segment_key(Partition* partition, PartitionSegment* segment) {
	return "";
}