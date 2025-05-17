#include "../../header_files/queue_management/SegmentLockManager.h"

SegmentLockManager::SegmentLockManager() {}

void SegmentLockManager::lock_segment(Partition* partition, PartitionSegment* segment, bool exclusive) {
	std::lock_guard<std::mutex> map_lock(this->mut);

	std::string key = this->get_segment_key(partition, segment);

	if (this->locks.find(key) == this->locks.end()) this->locks[key] = std::make_shared<segment_lock>();
	else
		this->locks[key].get()->references++;
	
	if (exclusive) this->locks[key].get()->mut.lock();
	else this->locks[key].get()->mut.lock_shared();
}

void SegmentLockManager::release_segment_lock(Partition* partition, PartitionSegment* segment, bool exclusive) {
	std::async(std::launch::async, [&] {
		std::lock_guard<std::mutex> map_lock(this->mut);

		std::string key = this->get_segment_key(partition, segment);

		if (this->locks.find(key) == this->locks.end()) return;

		if (exclusive) this->locks[key].get()->mut.unlock();
		else this->locks[key].get()->mut.unlock_shared();

		if ((--this->locks[key].get()->references) <= 0)
			this->locks.erase(key);
	});
}

const std::string& SegmentLockManager::get_segment_key(Partition* partition, PartitionSegment* segment) {
	return partition->get_queue_name() 
		+ "_" 
		+ std::to_string(partition->get_partition_id()) 
		+ (segment != NULL ? ("_" + std::to_string(segment->get_id())) : "");
}