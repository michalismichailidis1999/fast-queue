#include "../../../header_files/queue_management/messages_management/MessagesHandler.h"

MessagesHandler::MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm, SegmentAllocator* sa, BPlusTreeIndexHandler* index_handler, Settings* settings) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
	this->pm = pm;
	this->sa = sa;
	this->index_handler = index_handler;
	this->settings = settings;

	this->cluster_metadata_file_key = this->pm->get_metadata_file_key(CLUSTER_METADATA_QUEUE_NAME);
	this->cluster_metadata_file_path = this->pm->get_metadata_file_path(CLUSTER_METADATA_QUEUE_NAME);
}

void MessagesHandler::save_messages(Partition* partition, void* messages, unsigned int total_bytes) {
	if(partition->get_active_segment()->get_is_read_only())
		this->sa->allocate_new_segment(partition);

	PartitionSegment* active_segment = partition->get_active_segment();
	bool is_static_file = Helper::is_internal_queue(partition->get_queue_name());

	long long first_message_pos = this->disk_flusher->flush_data_to_disk(
		active_segment->get_segment_key(), 
		active_segment->get_segment_path(), 
		messages, 
		-1, 
		is_static_file
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
		this->index_handler->add_message_to_index(partition, first_message_id, first_message_pos);
	}
}

void MessagesHandler::update_cluster_metadata_commit_index(unsigned long long commit_index) {
	this->update_cluster_metadata_index_value(commit_index, QUEUE_LAST_COMMIT_INDEX_SIZE, QUEUE_LAST_COMMIT_INDEX_OFFSET);
}

void MessagesHandler::update_cluster_metadata_last_applied(unsigned long long last_applied) {
	this->update_cluster_metadata_index_value(last_applied, QUEUE_LAST_APPLIED_INDEX_SIZE, QUEUE_LAST_APPLIED_INDEX_OFFSET);
}

void MessagesHandler::update_cluster_metadata_index_value(unsigned long long index_value, unsigned int index_size, unsigned int index_pos) {
	this->disk_flusher->flush_data_to_disk(
		this->cluster_metadata_file_key,
		this->cluster_metadata_file_path,
		&index_value,
		index_size,
		index_pos,
		true,
		true
	);
}

std::string MessagesHandler::get_queue_partition_key(Partition* partition) {
	return partition->get_queue_name()
		+ "-" + std::to_string(partition->get_partition_id()) 
		+ "-" + std::to_string(partition->get_current_segment());
}

unsigned long MessagesHandler::remove_from_partition_remaining_bytes(const std::string& queue_partition_key, unsigned int bytes_written) {
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