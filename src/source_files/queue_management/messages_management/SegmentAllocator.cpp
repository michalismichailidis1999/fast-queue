#include "../../../header_files/queue_management/messages_management/SegmentAllocator.h"

SegmentAllocator::SegmentAllocator(SegmentMessageMap* smm, SegmentLockManager* lock_manager, QueueSegmentFilePathMapper* pm, DiskFlusher* df) {
	this->smm = smm;
	this->lock_manager = lock_manager;
	this->pm = pm;
	this->df = df;
}

void SegmentAllocator::allocate_new_segment(Partition* partition) {
	PartitionSegment* segment = partition->get_active_segment();

	this->lock_manager->lock_segment(partition, segment);

	try
	{
		segment->set_to_read_only();

		this->df->flush_metadata_updates_to_disk(segment);

		unsigned long long new_segment_id = partition->get_current_segment_id() + 1;

		std::string new_segment_key = this->pm->get_file_key(partition->get_queue_name(), new_segment_id);
		std::string new_segment_path = this->pm->get_file_path(partition->get_queue_name(), new_segment_id);

		std::string new_segment_index_key = this->pm->get_file_key(partition->get_queue_name(), new_segment_id, true);
		std::string new_segment_index_path = this->pm->get_file_path(partition->get_queue_name(), new_segment_id, true);

		std::shared_ptr<PartitionSegment> new_segment = std::shared_ptr<PartitionSegment>(
			new PartitionSegment(new_segment_id, new_segment_key, new_segment_path)
		);

		new_segment.get()->set_index(new_segment_index_key, new_segment_index_path);

		this->df->flush_new_metadata_to_disk(new_segment.get(), segment->get_segment_key(), segment->get_index_key());

		if (new_segment_id > 1)
			this->smm->add_last_message_info_to_segment_map(partition, segment);

		partition->set_active_segment(new_segment);
	}
	catch (const std::exception& ex)
	{
		// TODO: Add logging here
	}

	this->lock_manager->release_segment_lock(partition, segment);
}