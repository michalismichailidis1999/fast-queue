#include "../../../header_files/queue_management/messages_management/MessagesHandler.h"

MessagesHandler::MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm, SegmentAllocator* sa, Settings* settings) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
	this->pm = pm;
	this->sa = sa;
	this->settings = settings;
}

void MessagesHandler::save_messages(Partition* partition, void* messages, unsigned long total_bytes) {
	if(partition->get_active_segment()->get_is_read_only())
		this->sa->allocate_new_segment(partition);

	const std::string& queue_name = partition->get_queue_name();
	unsigned long long current_segment = partition->get_current_segment();
	unsigned int partition_id = partition->get_partition_id();

	std::string file_key = this->pm->get_file_key(queue_name, current_segment);
	std::string file_path = this->pm->get_file_path(queue_name, current_segment, partition_id);

	long long first_message_pos = this->disk_flusher->flush_data_to_disk(
		file_key, file_path, messages, -1, Helper::is_internal_queue(queue_name)
	);

	unsigned long total_segment_bytes = partition->get_active_segment()->add_written_bytes(total_bytes);

	//if (this->settings->get_segment_size() < total_segment_bytes)
	//	this->sa->allocate_new_segment(partition);

	if (100 < total_segment_bytes) {
		this->sa->allocate_new_segment(partition);
	}

	if (this->remove_from_partition_remaining_bytes(this->get_queue_partition_key(partition), total_bytes) == 0) {
		unsigned long long first_message_id = 0;
		memcpy_s(&first_message_id, MESSAGE_ID_SIZE, (char*)messages + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
		this->update_segment_index(partition->get_active_segment(), first_message_id, first_message_pos);
	}
}

void MessagesHandler::update_cluster_metadata_commit_index(unsigned long long commit_index) {
	this->update_cluster_metadata_index_value(commit_index, QUEUE_LAST_COMMIT_INDEX_SIZE, QUEUE_LAST_COMMIT_INDEX_OFFSET);
}

void MessagesHandler::update_cluster_metadata_last_applied(unsigned long long last_applied) {
	this->update_cluster_metadata_index_value(last_applied, QUEUE_LAST_APPLIED_INDEX_SIZE, QUEUE_LAST_APPLIED_INDEX_OFFSET);
}

void MessagesHandler::update_cluster_metadata_index_value(unsigned long long index_value, unsigned long index_size, unsigned long index_pos) {
	std::string file_key = this->pm->get_metadata_file_key(CLUSTER_METADATA_QUEUE_NAME);
	std::string file_path = this->pm->get_metadata_file_path(CLUSTER_METADATA_QUEUE_NAME);

	this->disk_flusher->flush_data_to_disk(
		file_key,
		file_path,
		&index_value,
		index_size,
		index_pos,
		true,
		true
	);
}

void MessagesHandler::update_segment_index(PartitionSegment* segment, unsigned long long message_id, long long message_pos) {

}

std::string MessagesHandler::get_queue_partition_key(Partition* partition) {
	return partition->get_queue_name()
		+ "-" + std::to_string(partition->get_partition_id()) 
		+ "-" + std::to_string(partition->get_current_segment());
}

unsigned long MessagesHandler::remove_from_partition_remaining_bytes(const std::string& queue_partition_key, unsigned long bytes_written) {
	std::lock_guard<std::mutex> lock(this->remaining_bytes_mut);

	unsigned long remaining_bytes = this->remaining_bytes[queue_partition_key];

	if (remaining_bytes == 0) {
		this->remaining_bytes[queue_partition_key] = 50; // TODO: Add this value to settings or add it in queue creation
		return 0;
	}

	remaining_bytes = remaining_bytes > bytes_written ? remaining_bytes - bytes_written : 0;

	this->remaining_bytes[queue_partition_key] = remaining_bytes;

	return remaining_bytes;
}