#include "../../../header_files/queue_management/messages_management/MessagesHandler.h"

MessagesHandler::MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm, SegmentAllocator* sa, Settings* settings) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
	this->pm = pm;
	this->sa = sa;
	this->settings = settings;
}

void MessagesHandler::save_messages(Partition* partition, void* messages, long total_bytes) {
	const std::string& queue_name = partition->get_queue_name();
	unsigned long long current_segment = partition->get_current_segment();
	unsigned int partition_id = partition->get_partition_id();

	std::string file_key = this->pm->get_file_key(queue_name, current_segment);
	std::string file_path = this->pm->get_file_path(queue_name, current_segment, partition_id);

	this->disk_flusher->flush_data_to_disk(file_key, file_path, messages, -1, Helper::is_internal_queue(queue_name));

	long total_segment_bytes = partition->get_active_segment()->add_written_bytes(total_bytes);

	//if (this->settings->get_segment_size() < total_segment_bytes)
	//	this->sa->allocate_new_segment(partition);

	if (100 < total_segment_bytes) {
		this->sa->allocate_new_segment(partition);
	}
}