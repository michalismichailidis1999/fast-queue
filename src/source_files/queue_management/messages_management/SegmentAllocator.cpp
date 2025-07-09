#include "../../../header_files/queue_management/messages_management/SegmentAllocator.h"

SegmentAllocator::SegmentAllocator(SegmentMessageMap* smm, SegmentLockManager* lock_manager, QueueSegmentFilePathMapper* pm, DiskFlusher* df, Logger* logger) {
	this->smm = smm;
	this->lock_manager = lock_manager;
	this->pm = pm;
	this->df = df;
	this->logger = logger;
}

bool SegmentAllocator::allocate_new_segment(Partition* partition) {
	bool is_internal_queue = Helper::is_internal_queue(partition->get_queue_name());

	std::shared_ptr<PartitionSegment> segment = partition->get_active_segment_ref();
	std::shared_ptr<PartitionSegment> old_active_segment = nullptr;

	if (segment != nullptr) {
		this->lock_manager->lock_segment(partition, segment.get(), true);

		if (segment.get()->should_ignore_segment_allocation()) {
			this->lock_manager->release_segment_lock(partition, segment.get(), true);
			return false;
		}
	}

	try
	{
		if (segment != nullptr) {
			segment->set_to_read_only();

			this->df->flush_metadata_updates_to_disk(segment.get());
		}

		unsigned long long new_segment_id = partition->get_current_segment_id() + 1;

		std::string new_segment_key = this->pm->get_file_key(
			partition->get_queue_name(),
			new_segment_id,
			is_internal_queue ? -1 : partition->get_partition_id()
		);

		std::string new_segment_path = this->pm->get_file_path(
			partition->get_queue_name(), 
			new_segment_id,
			is_internal_queue ? -1 : partition->get_partition_id()
		);

		std::string new_segment_index_key = this->pm->get_file_key(
			partition->get_queue_name(), 
			new_segment_id,
			is_internal_queue ? -1 : partition->get_partition_id(),
			true
		);
		std::string new_segment_index_path = this->pm->get_file_path(
			partition->get_queue_name(), 
			new_segment_id,
			is_internal_queue ? -1 : partition->get_partition_id(),
			true
		);

		std::shared_ptr<PartitionSegment> new_segment = std::shared_ptr<PartitionSegment>(
			new PartitionSegment(new_segment_id, new_segment_key, new_segment_path)
		);

		new_segment.get()->set_index(new_segment_index_key, new_segment_index_path);

		this->df->flush_new_metadata_to_disk(
			new_segment.get(), 
			segment != NULL ? segment->get_segment_key() : "",
			segment != NULL ? segment->get_index_key() : ""
		);

		if (new_segment_id > 1)
			this->smm->add_last_message_info_to_segment_map(partition, segment.get());

		old_active_segment = partition->set_active_segment(new_segment);
	}
	catch (const std::exception& ex)
	{
		if (segment != nullptr)
			this->lock_manager->release_segment_lock(partition, segment.get(), true);

		throw ex;
	}

	if (segment != nullptr) {
		segment.get()->set_ignore_segment_allocation_to_true();

		this->lock_manager->release_segment_lock(partition, segment.get(), true);
	}

	return true;
}