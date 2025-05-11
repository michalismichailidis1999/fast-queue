#include "../../../header_files/queue_management/messages_management/MessagesHandler.h"

MessagesHandler::MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm, SegmentAllocator* sa, SegmentMessageMap* smm, BPlusTreeIndexHandler* index_handler, Settings* settings) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
	this->pm = pm;
	this->sa = sa;
	this->smm = smm;
	this->index_handler = index_handler;
	this->settings = settings;

	this->cluster_metadata_file_key = this->pm->get_metadata_file_key(CLUSTER_METADATA_QUEUE_NAME);
	this->cluster_metadata_file_path = this->pm->get_metadata_file_path(CLUSTER_METADATA_QUEUE_NAME);
}

void MessagesHandler::save_messages(Partition* partition, void* messages, unsigned int total_bytes) {
	if(partition->get_active_segment()->get_is_read_only())
		this->sa->allocate_new_segment(partition);

	PartitionSegment* active_segment = partition->get_active_segment();

	this->set_last_message_id_and_timestamp(active_segment, messages, total_bytes);

	long long first_message_pos = this->disk_flusher->append_data_to_end_of_file(
		active_segment->get_segment_key(), 
		active_segment->get_segment_path(), 
		messages, 
		Helper::is_internal_queue(partition->get_queue_name())
	);

	unsigned long total_segment_bytes = partition->get_active_segment()->add_written_bytes(total_bytes);

	//if (this->settings->get_segment_size() < total_segment_bytes) {
	//	this->sa->allocate_new_segment(partition);
	//	return;
	//}

	if (100 < total_segment_bytes) {
		this->sa->allocate_new_segment(partition);
		return;
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
	this->disk_flusher->flush_metadata_updates_to_disk(
		this->cluster_metadata_file_key,
		this->cluster_metadata_file_path,
		&index_value,
		index_size,
		index_pos
	);
}

std::string MessagesHandler::get_queue_partition_key(Partition* partition) {
	return partition->get_queue_name()
		+ "-" + std::to_string(partition->get_partition_id()) 
		+ "-" + std::to_string(partition->get_current_segment_id());
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

void MessagesHandler::set_last_message_id_and_timestamp(PartitionSegment* segment, void* messages, unsigned int total_bytes) {
	unsigned long long last_message_offset = 0;
	long long last_message_timestamp = 0;

	unsigned int offset = 0;

	while (offset < total_bytes) {
		unsigned int message_total_bytes = 0;

		memcpy_s(&last_message_offset, SEGMENT_LAST_MESSAGE_OFF_SIZE, (char*)messages + SEGMENT_LAST_MESSAGE_OFF_OFFSET, SEGMENT_LAST_MESSAGE_OFF_SIZE);
		memcpy_s(&last_message_timestamp, SEGMENT_LAST_MESSAGE_TMSTMP_SIZE, (char*)messages + SEGMENT_LAST_MESSAGE_TMSTMP_OFFSET, SEGMENT_LAST_MESSAGE_TMSTMP_SIZE);

		offset += message_total_bytes;
	}

	segment->set_last_message_offset(last_message_offset);
	segment->set_last_message_timestamp(last_message_timestamp);
}

std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int> MessagesHandler::read_partition_messages(Partition* partition, unsigned long long read_from_message_id) {
	std::shared_ptr<PartitionSegment> old_segment = this->smm->find_message_segment(partition, read_from_message_id);

	PartitionSegment* segment_to_read = old_segment == nullptr
		? partition->get_active_segment()
		: old_segment.get();

	long long message_pos = this->index_handler->find_message_location(segment_to_read, read_from_message_id);

	if(message_pos <= 0) 
		return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);

	std::shared_ptr<char> read_batch = std::shared_ptr<char>(new char[READ_MESSAGES_BATCH_SIZE]);
	unsigned int batch_size = READ_MESSAGES_BATCH_SIZE;

	unsigned int message_id_offset = 0;
	unsigned int message_bytes = 0;
	unsigned int last_message_offset = 0;
	unsigned int last_message_bytes = 0;
	unsigned long long last_message_id = 0;

	while (true) {
		this->disk_reader->read_data_from_disk(
			segment_to_read->get_segment_key(),
			segment_to_read->get_segment_path(),
			read_batch.get(),
			batch_size,
			message_pos
		);

		message_id_offset = this->get_message_offset(read_batch.get(), batch_size, read_from_message_id);
		last_message_offset = this->get_last_message_offset_from_batch(read_batch.get(), batch_size);

		if (message_id_offset > 0 && message_id_offset < batch_size)
			memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + message_id_offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		memcpy_s(&last_message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + last_message_offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
		memcpy_s(&last_message_id, MESSAGE_ID_SIZE, read_batch.get() + last_message_offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

		if (message_id_offset == 0 && last_message_id > read_from_message_id)
			return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);

		if (message_id_offset == batch_size) {
			message_pos += last_message_offset + last_message_bytes;
			continue;
		}

		if (message_id_offset + message_bytes <= batch_size) break;

		message_pos += message_id_offset;
		batch_size = message_bytes > READ_MESSAGES_BATCH_SIZE ? message_bytes : READ_MESSAGES_BATCH_SIZE;
		read_batch = std::shared_ptr<char>(new char[batch_size]);
	}

	unsigned int second_last_message_offset = this->get_second_last_message_offset_from_batch(
		read_batch.get(),
		batch_size,
		message_id_offset,
		last_message_offset
	);

	unsigned int second_last_message_bytes = 0;
	memcpy_s(&second_last_message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + second_last_message_offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

	unsigned int read_start = message_id_offset;
	unsigned int read_end = last_message_offset + last_message_bytes > batch_size
		? second_last_message_offset + second_last_message_bytes
		: last_message_offset + last_message_bytes;

	return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(
		read_batch, 
		batch_size, 
		read_start, 
		read_end,
		this->get_total_messages_read(read_batch.get(), batch_size, read_start, read_end)
	);
}

unsigned int MessagesHandler::get_message_offset(void* read_batch, unsigned int batch_size, unsigned long long message_id) {
	unsigned long long current_message_id = 0;
	unsigned int message_bytes = 0;
	unsigned int offset = 0;

	while (offset < batch_size) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
		memcpy_s(&current_message_id, MESSAGE_ID_SIZE, (char*)read_batch + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

		if (message_id == current_message_id) return offset;

		offset += message_bytes;
	}

	return batch_size;
}

unsigned int MessagesHandler::get_last_message_offset_from_batch(void* read_batch, unsigned int batch_size) {
	unsigned int message_bytes = 0;
	unsigned int offset = 0;

	while (offset < batch_size) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		if (offset + message_bytes >= batch_size) return offset;

		offset += message_bytes;
	}

	return offset;
}

unsigned int MessagesHandler::get_total_messages_read(void* read_batch, unsigned int batch_size, unsigned int read_start, unsigned int read_end) {
	unsigned int message_bytes = 0;
	unsigned int messages_read = 0;
	unsigned int offset = 0;

	while (offset < batch_size) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		if (offset + message_bytes > batch_size) break;

		messages_read++;
		offset += message_bytes;
	}

	return messages_read;
}

unsigned int MessagesHandler::get_second_last_message_offset_from_batch(void* read_batch, unsigned int batch_size, unsigned int starting_offset, unsigned int ending_offset) {
	unsigned int message_bytes = 0;
	unsigned int offset = starting_offset;

	while (offset < ending_offset) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		if (offset + message_bytes >= ending_offset) return offset;

		offset += message_bytes;
	}

	return offset;
}