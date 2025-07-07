#include "../../../header_files/queue_management/messages_management/MessagesHandler.h"

MessagesHandler::MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm, SegmentAllocator* sa, SegmentMessageMap* smm, SegmentLockManager* lock_manager, BPlusTreeIndexHandler* index_handler, Util* util, Settings* settings, Logger* logger) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
	this->pm = pm;
	this->sa = sa;
	this->smm = smm;
	this->lock_manager = lock_manager;
	this->index_handler = index_handler;
	this->util = util;
	this->settings = settings;
	this->logger = logger;

	this->cluster_metadata_file_key = this->pm->get_metadata_file_key(CLUSTER_METADATA_QUEUE_NAME);
	this->cluster_metadata_file_path = this->pm->get_metadata_file_path(CLUSTER_METADATA_QUEUE_NAME);
}

bool MessagesHandler::save_messages(Partition* partition, ProduceMessagesRequest* request) {
	unsigned int total_messages_bytes = 0;

	for(auto& s : *(request->messages_sizes.get()))
		total_messages_bytes += s + MESSAGE_TOTAL_BYTES;

	std::unique_ptr<char> messages_data = std::unique_ptr<char>(new char[total_messages_bytes]);

	unsigned int offset = 0;
	unsigned long long current_timestamp = this->util->get_current_time_milli().count();

	for (int i = 0; i < request->messages.get()->size(); i++) {
		auto& message_size = (*(request->messages_sizes.get()))[i];
		auto& message_body = (*(request->messages.get()))[i];
		auto& message_key = (*(request->messages_keys.get()))[i];
		auto& message_key_size = (*(request->messages_keys_sizes.get()))[i];

		unsigned long long message_offset = partition->get_next_message_offset();

		memcpy_s(messages_data.get() + offset + MESSAGE_TOTAL_BYTES, message_size, message_body, message_size);

		Helper::add_message_metadata_values(messages_data.get() + offset + MESSAGE_TOTAL_BYTES, message_offset, current_timestamp, message_key_size, message_key);

		Helper::add_common_metadata_values(messages_data.get() + offset, message_size + MESSAGE_TOTAL_BYTES, ObjectType::MESSAGE);

		offset += MESSAGE_TOTAL_BYTES + message_size;
	}

	return this->save_messages(partition, messages_data.get(), total_messages_bytes);
}

// No segment locking is required here since retention/compaction will only happen in read only segments
bool MessagesHandler::save_messages(Partition* partition, void* messages, unsigned int total_bytes) {
	try
	{
		if (partition->get_active_segment()->get_is_read_only() && !partition->get_active_segment()->is_segment_compacted())
			this->sa->allocate_new_segment(partition);

		PartitionSegment* active_segment = partition->get_active_segment();

		this->set_last_message_id_and_timestamp(active_segment, messages, total_bytes);

		unsigned long long first_message_pos = this->disk_flusher->append_data_to_end_of_file(
			active_segment->get_segment_key(),
			active_segment->get_segment_path(),
			messages,
			total_bytes,
			Helper::is_internal_queue(partition->get_queue_name())
		);

		unsigned long total_segment_bytes = partition->get_active_segment()->add_written_bytes(total_bytes);

		//if (this->settings->get_segment_size() < total_segment_bytes) {
		//	this->sa->allocate_new_segment(partition);
		//	return;
		//}

		if (100 < total_segment_bytes) {
			this->sa->allocate_new_segment(partition);
			return true;
		}

		if (this->remove_from_partition_remaining_bytes(this->get_queue_partition_key(partition), total_bytes) == 0) {
			unsigned long long first_message_id = 0;
			memcpy_s(&first_message_id, MESSAGE_ID_SIZE, (char*)messages + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
			this->index_handler->add_message_to_index(partition, first_message_id, first_message_pos);
		}

		return true;
	}
	catch (const CorruptionException& ex)
	{
		// TODO: Mark corruption for fix
		this->logger->log_error(ex.what());
		return false;
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

		memcpy_s(&message_total_bytes, TOTAL_METADATA_BYTES, (char*)messages + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
		memcpy_s(&last_message_offset, MESSAGE_ID_SIZE, (char*)messages + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
		memcpy_s(&last_message_timestamp, MESSAGE_TIMESTAMP_SIZE, (char*)messages + offset + MESSAGE_TIMESTAMP_OFFSET, MESSAGE_TIMESTAMP_SIZE);

		offset += message_total_bytes;
	}

	segment->set_last_message_offset(last_message_offset);
	segment->set_last_message_timestamp(last_message_timestamp);
}

std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int> MessagesHandler::read_partition_messages(Partition* partition, unsigned long long read_from_message_id, unsigned int maximum_messages_to_read) {
	PartitionSegment* segment_to_read = NULL;

	try
	{
		bool success = true;

		std::shared_ptr<PartitionSegment> old_segment = this->smm->find_message_segment(partition, read_from_message_id, &success);

		if (old_segment == nullptr && !success)
			return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);

		PartitionSegment* segment_to_read = old_segment == nullptr
			? partition->get_active_segment()
			: old_segment.get();

		this->lock_manager->lock_segment(partition, segment_to_read);

		if (segment_to_read->get_id() < partition->get_smallest_segment_id()) {
			this->lock_manager->release_segment_lock(partition, segment_to_read);
			return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
		}

		long long message_pos = this->index_handler->find_message_location(segment_to_read, read_from_message_id);

		if (message_pos <= 0) {
			this->lock_manager->release_segment_lock(partition, segment_to_read);
			return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
		}

		std::shared_ptr<char> read_batch = std::shared_ptr<char>(new char[READ_MESSAGES_BATCH_SIZE]);
		unsigned int batch_size = READ_MESSAGES_BATCH_SIZE;

		unsigned int message_offset = 0;
		unsigned int message_bytes = 0;
		unsigned int last_message_offset = 0;
		unsigned int last_message_bytes = 0;
		unsigned long long last_message_id = 0;

		bool message_found = false;
		bool is_message_active = true;

		while (true) {
			batch_size = this->disk_reader->read_data_from_disk(
				segment_to_read->get_segment_key(),
				segment_to_read->get_segment_path(),
				read_batch.get(),
				batch_size,
				message_pos
			);

			if (batch_size == 0) {
				this->lock_manager->release_segment_lock(partition, segment_to_read);
				return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
			}

			message_offset = this->get_message_offset(read_batch.get(), batch_size, read_from_message_id, &message_found);
			last_message_offset = this->get_last_message_offset_from_batch(read_batch.get(), batch_size);

			if (message_found)
			{
				memcpy_s(&is_message_active, MESSAGE_IS_ACTIVE_SIZE, read_batch.get() + message_offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);
				
				if (!is_message_active) {
					this->lock_manager->release_segment_lock(partition, segment_to_read);
					return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
				}

				memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + message_offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
			}

			memcpy_s(&last_message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + last_message_offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
			memcpy_s(&last_message_id, MESSAGE_ID_SIZE, read_batch.get() + last_message_offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

			if (!message_found && last_message_id > read_from_message_id) {
				this->lock_manager->release_segment_lock(partition, segment_to_read);
				return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
			}

			if (message_offset >= batch_size - MESSAGE_TOTAL_BYTES) {
				message_pos += last_message_offset + last_message_bytes;
				continue;
			}

			if (message_offset + message_bytes <= batch_size) break;

			message_pos += message_offset;
			batch_size = message_bytes > READ_MESSAGES_BATCH_SIZE ? message_bytes : READ_MESSAGES_BATCH_SIZE;
			read_batch = std::shared_ptr<char>(new char[batch_size]);
		}

		this->lock_manager->release_segment_lock(partition, segment_to_read);

		unsigned int second_last_message_offset = this->get_second_last_message_offset_from_batch(
			read_batch.get(),
			batch_size,
			message_offset,
			last_message_offset
		);

		unsigned int second_last_message_bytes = 0;
		memcpy_s(&second_last_message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + second_last_message_offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		unsigned int read_start = message_offset;
		unsigned int read_end = last_message_offset + last_message_bytes > batch_size
			? second_last_message_offset + second_last_message_bytes
			: last_message_offset + last_message_bytes;

		message_bytes = 0;
		is_message_active = true;

		unsigned int total_read = 0;
		unsigned int new_read_end = read_start;

		while (new_read_end < read_end && (maximum_messages_to_read > 0 ? total_read < maximum_messages_to_read : true)) {
			memcpy_s(&is_message_active, MESSAGE_IS_ACTIVE_SIZE, read_batch.get() + new_read_end + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);

			if (!is_message_active) break;

			memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + new_read_end + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
			new_read_end += message_bytes;
			total_read++;
		}

		read_end = new_read_end;

		return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(
			read_batch,
			batch_size,
			read_start,
			read_end,
			total_read
		);
	}
	catch (const CorruptionException& ex)
	{
		// TODO: Mark corruption for fix
		this->logger->log_error(ex.what());
		this->lock_manager->release_segment_lock(partition, segment_to_read);
		return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
	}
	catch (const std::exception& ex)
	{
		this->logger->log_error(ex.what());
		this->lock_manager->release_segment_lock(partition, segment_to_read);
		return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
	}
}

unsigned int MessagesHandler::get_message_offset(void* read_batch, unsigned int batch_size, unsigned long long message_id, bool* message_found, bool message_can_have_larger_id) {
	unsigned long long current_message_id = 0;
	unsigned int message_bytes = 0;
	unsigned int offset = 0;

	while (offset <= batch_size - MESSAGE_TOTAL_BYTES) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
		memcpy_s(&current_message_id, MESSAGE_ID_SIZE, (char*)read_batch + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

		if (message_id == current_message_id && !message_can_have_larger_id) {
			*message_found = true;
			return offset;
		}

		if (message_id >= current_message_id && message_can_have_larger_id) {
			*message_found = true;
			return offset;
		}

		offset += message_bytes;
	}

	return batch_size;
}

unsigned int MessagesHandler::get_last_message_offset_from_batch(void* read_batch, unsigned int batch_size) {
	unsigned int message_bytes = 0;
	unsigned int offset = 0;

	while (offset <= batch_size - MESSAGE_TOTAL_BYTES) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		if (offset + message_bytes >= batch_size) return offset;

		if (!Helper::has_valid_checksum((char*)read_batch + offset))
			throw CorruptionException("Messages batch had corrupted message");

		offset += message_bytes;
	}

	return offset;
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

bool MessagesHandler::remove_messages_after_message_id(Partition* partition, unsigned long long message_id) {
	PartitionSegment* segment_to_read = NULL;

	try
	{
		bool success = true;

		std::shared_ptr<PartitionSegment> old_segment = this->smm->find_message_segment(partition, message_id, &success);

		if (old_segment == nullptr && !success) {
			this->lock_manager->release_segment_lock(partition, segment_to_read);
			return false;
		}

		PartitionSegment* segment_to_read = old_segment == nullptr
			? partition->get_active_segment()
			: old_segment.get();

		this->lock_manager->lock_segment(partition, segment_to_read, true);

		if (segment_to_read->get_id() < partition->get_smallest_segment_id()) {
			this->lock_manager->release_segment_lock(partition, segment_to_read);
			return false;
		}

		long long message_pos = this->index_handler->find_message_location(segment_to_read, message_id, true);

		if (message_pos <= 0) {
			this->lock_manager->release_segment_lock(partition, segment_to_read);
			return false;
		}

		std::shared_ptr<char> read_batch = std::shared_ptr<char>(new char[READ_MESSAGES_BATCH_SIZE]);
		unsigned int batch_size = READ_MESSAGES_BATCH_SIZE;

		unsigned int message_offset = 0;

		bool message_found = false;

		while (true) {
			batch_size = this->disk_reader->read_data_from_disk(
				segment_to_read->get_segment_key(),
				segment_to_read->get_segment_path(),
				read_batch.get(),
				batch_size,
				message_pos
			);

			if (batch_size == 0) break;

			message_found = false;
			message_offset = this->get_message_offset(read_batch.get(), batch_size, message_id, &message_found, true);

			if (message_found) {
				unsigned int message_bytes = 0;
				unsigned int changed = 0;
				bool is_message_active = true;

				unsigned int offset = message_offset;

				while (offset <= batch_size - MESSAGE_TOTAL_BYTES) {
					memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
					memcpy_s(&is_message_active, MESSAGE_IS_ACTIVE_SIZE, read_batch.get() + offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);

					if (is_message_active) {
						is_message_active = false;
						memcpy_s(read_batch.get() + offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE, &is_message_active, MESSAGE_IS_ACTIVE_SIZE);
						changed++;
					}

					offset += message_bytes;
				}

				if(changed > 0)
					this->disk_flusher->write_data_to_specific_file_location(
						segment_to_read->get_segment_key(),
						segment_to_read->get_segment_path(),
						read_batch.get(),
						batch_size,
						message_pos,
						true
					);

				message_pos += offset;
			}

			if (batch_size < READ_MESSAGES_BATCH_SIZE) break;
		}

		return true;
	}
	catch (const CorruptionException& ex)
	{
		// TODO: Mark corruption for fix
		this->logger->log_error(ex.what());
		this->lock_manager->release_segment_lock(partition, segment_to_read, true);
		return false;
	}
	catch (const std::exception& ex)
	{
		this->logger->log_error(ex.what());
		this->lock_manager->release_segment_lock(partition, segment_to_read, true);
		return false;
	}
}