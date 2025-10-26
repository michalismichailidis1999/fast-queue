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

void MessagesHandler::set_transaction_handler(TransactionHandler* th) {
	this->th = th;
}

bool MessagesHandler::save_messages(Partition* partition, ProduceMessagesRequest* request, bool cache_messages, bool has_replication, unsigned long long leader_id) {
	std::lock_guard<std::mutex> lock(partition->write_mut);
	
	unsigned int total_messages_bytes = 0;

	for(auto& s : *(request->messages_sizes.get()))
		total_messages_bytes += s + MESSAGE_TOTAL_BYTES;

	for (auto& s : *(request->messages_keys_sizes.get()))
		total_messages_bytes += s;

	std::unique_ptr<char> messages_data = std::unique_ptr<char>(new char[total_messages_bytes]);

	unsigned int offset = 0;
	unsigned long long current_timestamp = this->util->get_current_time_milli().count();

	unsigned long long message_offset = 0;

	bool is_commited = request->transaction_id == 0;

	for (int i = 0; i < request->messages.get()->size(); i++) {
		auto& message_size = (*(request->messages_sizes.get()))[i];
		auto& message_body = (*(request->messages.get()))[i];
		auto& message_key = (*(request->messages_keys.get()))[i];
		auto& message_key_size = (*(request->messages_keys_sizes.get()))[i];

		message_offset = partition->get_next_message_offset();

		memcpy_s(messages_data.get() + offset + MESSAGE_PAYLOAD_OFFSET, MESSAGE_PAYLOAD_SIZE, &message_size, MESSAGE_PAYLOAD_SIZE);
		memcpy_s(messages_data.get() + offset + MESSAGE_TOTAL_BYTES + message_key_size, message_size, message_body, message_size);
		memcpy_s(messages_data.get() + offset + MESSAGE_IS_COMMITED_OFFSET, MESSAGE_IS_COMMITED_SIZE, &is_commited, MESSAGE_IS_COMMITED_OFFSET);

		Helper::add_message_metadata_values(messages_data.get() + offset, message_offset, current_timestamp, message_key_size, message_key, MESSAGE_TOTAL_BYTES, leader_id);

		Helper::add_common_metadata_values(messages_data.get() + offset, message_key_size + message_size + MESSAGE_TOTAL_BYTES);

		offset += MESSAGE_TOTAL_BYTES + message_key_size + message_size;
	}

	partition->set_last_message_leader_epoch(leader_id);

	bool success = this->save_messages(partition, messages_data.get(), total_messages_bytes, nullptr, cache_messages);

	if (success && !has_replication)
	{
		std::lock_guard<std::shared_mutex> lock2(partition->consumers_mut);

		if(this->disk_flusher->path_exists(partition->get_offsets_path()))
			this->disk_flusher->write_data_to_specific_file_location(
				partition->get_offsets_key(),
				partition->get_offsets_path(),
				&message_offset,
				sizeof(unsigned long long),
				0,
				true
			);
	}

	return success;
}

bool MessagesHandler::save_messages(Partition* partition, void* messages, unsigned int total_bytes, std::shared_ptr<PartitionSegment> segment_to_write, bool cache_messages, unsigned long long transaction_group_id, unsigned long long transaction_id) {
	std::shared_ptr<PartitionSegment> active_segment = segment_to_write;
	
	try
	{
		active_segment = active_segment == nullptr ? partition->get_active_segment_ref() : active_segment;

		if (segment_to_write == nullptr && active_segment.get()->get_is_read_only() && !partition->get_active_segment()->is_segment_compacted()) {
			bool changed = this->sa->allocate_new_segment(partition);
			if(!changed) active_segment = partition->get_active_segment_ref();
		}

		if(segment_to_write == nullptr)
			this->lock_manager->lock_segment(partition, active_segment.get(), true);

		this->set_last_message_id_and_timestamp(active_segment.get(), messages, total_bytes);

		CacheKeyInfo cache_key_info = {
			partition->get_queue_name(), 
			partition->get_partition_id(), 
			active_segment.get()->get_id(),
			0
		};

		long long first_message_pos = this->disk_flusher->append_data_to_end_of_file(
			active_segment.get()->get_segment_key(),
			active_segment.get()->get_segment_path(),
			messages,
			total_bytes,
			Helper::is_internal_queue(partition->get_queue_name()),
			true,
			cache_messages ? &cache_key_info : NULL
		);

		if (first_message_pos == -1) {
			this->logger->log_error("Something went wrong while trying to append messages to end of segment");
			return false;
		}

		unsigned long total_segment_bytes = active_segment.get()->add_written_bytes(total_bytes);
		unsigned long long first_message_id = 0;

		if (transaction_group_id > 0 && transaction_id > 0) {
			memcpy_s(&first_message_id, MESSAGE_ID_SIZE, (char*)messages + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

			TransactionChangeCapture change_capture = TransactionChangeCapture{
				first_message_pos, 
				first_message_pos + total_bytes,
				first_message_id,
				transaction_id,
				transaction_group_id,
				partition->get_queue_name(),
				partition->get_partition_id()
			};

			this->th->capture_transaction_changes(partition, change_capture);
		}

		if (segment_to_write == nullptr && this->settings->get_segment_size() < total_segment_bytes) {
			if (segment_to_write == nullptr)
				this->lock_manager->release_segment_lock(partition, active_segment.get(), true);
			this->sa->allocate_new_segment(partition);
			return true;
		}

		if (this->remove_from_partition_remaining_bytes(this->get_queue_partition_key(partition), total_bytes) == 0) {
			memcpy_s(&first_message_id, MESSAGE_ID_SIZE, (char*)messages + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
			this->index_handler->add_message_to_index(partition, first_message_id, first_message_pos, cache_messages, segment_to_write.get());
		}

		if (segment_to_write == nullptr)
			this->lock_manager->release_segment_lock(partition, active_segment.get(), true);

		return true;
	}
	catch (const CorruptionException& ex)
	{
		if (segment_to_write == nullptr)
			this->lock_manager->release_segment_lock(partition, active_segment.get(), true);
		this->logger->log_error(ex.what());
		return false;
	}
	catch (const std::exception& ex)
	{
		if (segment_to_write == nullptr)
			this->lock_manager->release_segment_lock(partition, active_segment.get(), true);
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
		this->remaining_bytes[queue_partition_key] = this->settings->get_index_message_gap_size();
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

std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int> MessagesHandler::read_partition_messages(Partition* partition, unsigned long long read_from_message_id, unsigned int maximum_messages_to_read, bool get_prev_available, bool get_next_available, unsigned long long maximum_message_id) {
	if (read_from_message_id == 0)
		return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
	
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

		std::shared_ptr<char> cached_message = this->disk_reader->read_message_from_cache(partition, segment_to_read, read_from_message_id);

		if (cached_message != nullptr) {
			this->lock_manager->release_segment_lock(partition, segment_to_read);
			
			unsigned int message_bytes = 0;

			memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, cached_message.get() + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

			if(message_bytes > READ_MESSAGES_BATCH_SIZE || maximum_messages_to_read == 1)
				return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(cached_message, message_bytes, 0, message_bytes, 1);

			std::shared_ptr<char> cached_messages_batch = std::shared_ptr<char>(new char[READ_MESSAGES_BATCH_SIZE]);

			memcpy_s(cached_messages_batch.get(), message_bytes, cached_message.get(), message_bytes);

			unsigned int total_bytes = message_bytes;
			unsigned int total_messages = 1;
			unsigned long long message_id = 0;

			read_from_message_id++;

			while (
				total_bytes < READ_MESSAGES_BATCH_SIZE 
				&& (maximum_messages_to_read == 0 ? true : total_messages < maximum_messages_to_read)
			) {
				cached_message = this->disk_reader->read_message_from_cache(partition, segment_to_read, read_from_message_id);

				if (cached_message == nullptr) break;

				memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, cached_message.get() + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
				memcpy_s(&message_id, MESSAGE_ID_SIZE, cached_message.get() + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

				if (total_bytes + message_bytes > READ_MESSAGES_BATCH_SIZE) break;

				if (message_id >= maximum_messages_to_read) break;

				memcpy_s(cached_messages_batch.get() + total_bytes, message_bytes, cached_message.get(), message_bytes);

				total_bytes += message_bytes;
				total_messages++;
				read_from_message_id++;
			}

			return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(cached_messages_batch, total_bytes, 0, total_bytes, total_messages);
		}

		long long message_pos = this->index_handler->find_message_location(partition, segment_to_read, read_from_message_id);

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

		// last active message with message id < than the one searching for
		std::shared_ptr<char> prev_last_active_message = nullptr;
		std::shared_ptr<char> last_active_message = nullptr;

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
			memcpy_s(&last_message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + last_message_offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
			memcpy_s(&last_message_id, MESSAGE_ID_SIZE, read_batch.get() + last_message_offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

			prev_last_active_message = last_active_message;
			last_active_message = get_prev_available
				? this->get_last_active_message_less_than_message_id(read_batch.get(), batch_size, read_from_message_id)
				: nullptr;

			if (prev_last_active_message != nullptr && last_active_message == nullptr)
				last_active_message = prev_last_active_message;

			if (message_found) {
				memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + message_offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
				memcpy_s(&is_message_active, MESSAGE_IS_ACTIVE_SIZE, read_batch.get() + message_offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);

				if (is_message_active && message_bytes > batch_size) {
					batch_size = message_bytes;
					read_batch = std::shared_ptr<char>(new char[batch_size]);
					message_pos += message_offset;
					continue;
				}

				if (!is_message_active && !get_prev_available && !get_next_available) {
					this->lock_manager->release_segment_lock(partition, segment_to_read);
					return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
				}
				else if (!is_message_active && get_prev_available) {
					this->lock_manager->release_segment_lock(partition, segment_to_read);

					if(last_active_message == nullptr)
						return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);

					memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, last_active_message.get() + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

					return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(
						last_active_message, message_bytes, 0, message_bytes, 1
					);
				}
				else if (!is_message_active && get_next_available)
					return this->get_next_available_message(partition, segment_to_read, message_pos, read_batch.get(), batch_size, read_from_message_id);
			}
			else if (last_message_id > read_from_message_id && !get_prev_available && !get_next_available) {
				this->lock_manager->release_segment_lock(partition, segment_to_read);
				return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
			}
			else if (last_message_id > read_from_message_id && get_prev_available) {
				this->lock_manager->release_segment_lock(partition, segment_to_read);

				if (last_active_message == nullptr)
					return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);

				memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, last_active_message.get() + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

				return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(
					last_active_message, message_bytes, 0, message_bytes, 1
				);
			}
			else if (last_message_id > read_from_message_id && get_next_available)
				return this->get_next_available_message(partition, segment_to_read, message_pos, read_batch.get(), batch_size, read_from_message_id);

			if (message_found && message_offset + message_bytes <= batch_size) break;

			if (message_bytes > batch_size) {
				batch_size = message_bytes;
				read_batch = std::shared_ptr<char>(new char[batch_size]);
			}
			else if (message_bytes < batch_size && batch_size > READ_MESSAGES_BATCH_SIZE) {
				batch_size = READ_MESSAGES_BATCH_SIZE;
				read_batch = std::shared_ptr<char>(new char[batch_size]);
			}

			message_pos += last_message_offset + last_message_bytes;
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
		unsigned long long current_message_id = 0;

		while (new_read_end < read_end && (maximum_messages_to_read > 0 ? total_read < maximum_messages_to_read : true)) {
			memcpy_s(&current_message_id, MESSAGE_ID_SIZE, read_batch.get() + new_read_end + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
			memcpy_s(&is_message_active, MESSAGE_IS_ACTIVE_SIZE, read_batch.get() + new_read_end + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);

			if (!Helper::has_valid_checksum(read_batch.get() + new_read_end))
				throw CorruptionException("Correpted message detected");

			if (!is_message_active || (current_message_id > maximum_message_id && maximum_message_id > 0)) break;

			memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + new_read_end + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
			new_read_end += message_bytes;
			total_read++;
		}

		read_end = new_read_end;

		if(read_end == read_start)
			return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);

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

std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int> MessagesHandler::get_next_available_message(Partition* partition, PartitionSegment* segment, long long current_pos, void* current_batch, unsigned int current_batch_size, unsigned long long message_id) {
	unsigned int message_bytes = 0;
	bool is_active = true;

	unsigned long long current_message_id = 0;

	unsigned int offset = 0;

	while (offset < current_batch_size - MESSAGE_TOTAL_BYTES) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)current_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
		memcpy_s(&is_active, MESSAGE_IS_ACTIVE_SIZE, (char*)current_batch + offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);
		memcpy_s(&current_message_id, MESSAGE_ID_SIZE, (char*)current_batch + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

		if (offset + message_bytes <= current_batch_size && !Helper::has_valid_checksum((char*)current_batch + offset))
			throw CorruptionException("Corrupted message detected");

		if (message_id < current_message_id && is_active && message_bytes + offset < current_batch_size - MESSAGE_TOTAL_BYTES) {
			this->lock_manager->release_segment_lock(partition, segment);
			std::shared_ptr<char> active_message = std::shared_ptr<char>(new char[message_bytes]);
			memcpy_s(active_message.get(), message_bytes, (char*)current_batch + offset, message_bytes);
			return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(
				active_message, message_bytes, 0, message_bytes, 1
			);
		}

		offset += message_bytes;
	}

	current_pos += offset;

	std::shared_ptr<char> read_batch = std::shared_ptr<char>(new char[READ_MESSAGES_BATCH_SIZE]);
	unsigned int batch_size = READ_MESSAGES_BATCH_SIZE;

	unsigned int start_pos = 0;
	unsigned int end_pos = 0;

	unsigned int total_read = 0;

	bool first_active_found = false;

	while (true) {
		batch_size = this->disk_reader->read_data_from_disk(
			segment->get_segment_key(),
			segment->get_segment_path(),
			read_batch.get(),
			batch_size,
			current_pos
		);

		if (batch_size == 0) {
			this->lock_manager->release_segment_lock(partition, segment);
			return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(nullptr, 0, 0, 0, 0);
		}

		offset = 0;

		while (offset < batch_size - MESSAGE_TOTAL_BYTES) {
			memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
			memcpy_s(&is_active, MESSAGE_IS_ACTIVE_SIZE, read_batch.get() + offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);

			if (offset + message_bytes >= batch_size - MESSAGE_TOTAL_BYTES) break;

			if (!Helper::has_valid_checksum(read_batch.get() + offset))
				throw CorruptionException("Corrupted message detected");

			if (is_active) {
				if (!first_active_found) start_pos = offset;
				first_active_found = true;
				end_pos = offset + message_bytes;
				total_read++;
			}
			else if (first_active_found) break;

			offset += message_bytes;
		}

		if (first_active_found) break;

		if (message_bytes > batch_size) {
			batch_size = message_bytes;
			read_batch = std::shared_ptr<char>(new char[batch_size]);
		}
		else if (message_bytes < batch_size && batch_size > READ_MESSAGES_BATCH_SIZE) {
			batch_size = READ_MESSAGES_BATCH_SIZE;
			read_batch = std::shared_ptr<char>(new char[batch_size]);
		}

		current_pos += offset;
	}

	this->lock_manager->release_segment_lock(partition, segment);

	return std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int>(
		nullptr,
		end_pos - start_pos, 
		start_pos, 
		end_pos, 
		total_read
	);
}

std::shared_ptr<char> MessagesHandler::get_last_active_message_less_than_message_id(void* current_batch, unsigned int current_batch_size, unsigned long long message_id) {
	unsigned int message_bytes = 0;
	unsigned long long last_message_id = 0;

	bool is_active = true;

	unsigned int last_message_offset = 0;
	bool active_message_found = false;

	unsigned int offset = 0;

	while (offset < current_batch_size - MESSAGE_TOTAL_BYTES) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)current_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		if (offset + message_bytes > current_batch_size) break;

		memcpy_s(&last_message_id, MESSAGE_ID_SIZE, (char*)current_batch + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
		memcpy_s(&is_active, MESSAGE_IS_ACTIVE_SIZE, (char*)current_batch + offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);

		if (is_active && offset + message_bytes <= current_batch_size && message_id > last_message_id) {
			active_message_found = true;
			last_message_offset = offset;
		}

		if (message_id < last_message_id) break;

		offset += message_bytes;
	}

	if(!active_message_found) return nullptr;

	std::shared_ptr<char> last_active_message = std::shared_ptr<char>(new char[message_bytes]);

	memcpy_s(last_active_message.get(), message_bytes, (char*)current_batch + last_message_offset, message_bytes);

	return last_active_message;
}

unsigned int MessagesHandler::get_message_offset(void* read_batch, unsigned int batch_size, unsigned long long message_id, bool* message_found, bool message_can_have_larger_id) {
	unsigned long long current_message_id = 0;
	unsigned int message_bytes = 0;
	unsigned int offset = 0;

	while (offset <= batch_size - MESSAGE_TOTAL_BYTES) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
		memcpy_s(&current_message_id, MESSAGE_ID_SIZE, (char*)read_batch + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

		if (offset + message_bytes <= batch_size && !Helper::has_valid_checksum((char*)read_batch + offset))
			throw CorruptionException("Corrupted message detected");

		if (message_id == current_message_id) {
			*message_found = true;
			return offset;
		}

		if (message_id > current_message_id && message_can_have_larger_id) {
			*message_found = true;
			return offset;
		}

		offset += message_bytes;
	}

	return batch_size;
}

unsigned int MessagesHandler::get_last_message_offset_from_batch(void* read_batch, unsigned int batch_size, bool only_active_messages) {
	unsigned int message_bytes = 0;
	unsigned int offset = 0;

	bool is_active = true;
	unsigned int last_active_offset = 0;

	while (offset <= batch_size - MESSAGE_TOTAL_BYTES) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		if (offset + message_bytes > batch_size) return offset;

		if (!Helper::has_valid_checksum((char*)read_batch + offset))
			throw CorruptionException("Messages batch had corrupted message");

		memcpy_s(&is_active, MESSAGE_IS_ACTIVE_SIZE, (char*)read_batch + offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);

		if (is_active || !only_active_messages)
			last_active_offset = offset;

		offset += message_bytes;
	}

	return last_active_offset;
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
	std::shared_ptr<PartitionSegment> segment_to_read = nullptr;

	try
	{
		bool success = true;

		std::shared_ptr<PartitionSegment> old_segment = this->smm->find_message_segment(partition, message_id, &success);

		if (old_segment == nullptr && !success) return false;

		segment_to_read = old_segment == nullptr
			? partition->get_active_segment_ref()
			: old_segment;

		this->lock_manager->lock_segment(partition, segment_to_read.get(), true);

		if (segment_to_read->get_id() < partition->get_smallest_segment_id()) {
			this->lock_manager->release_segment_lock(partition, segment_to_read.get());
			return false;
		}

		long long message_pos = this->index_handler->find_message_location(partition, segment_to_read.get(), message_id, true);

		if (message_pos <= 0) {
			this->lock_manager->release_segment_lock(partition, segment_to_read.get());
			return false;
		}

		std::shared_ptr<char> read_batch = std::shared_ptr<char>(new char[READ_MESSAGES_BATCH_SIZE]);
		unsigned int batch_size = READ_MESSAGES_BATCH_SIZE;

		unsigned int message_offset = 0;

		bool message_found = false;

		while (segment_to_read != nullptr) {
			batch_size = this->disk_reader->read_data_from_disk(
				segment_to_read->get_segment_key(),
				segment_to_read->get_segment_path(),
				read_batch.get(),
				batch_size,
				message_pos
			);

			if (batch_size == 0) {
				this->lock_manager->release_segment_lock(partition, segment_to_read.get());
				segment_to_read = this->get_next_segment_to_remove_messages(partition, segment_to_read.get()->get_id());
				continue;
			}

			message_found = false;
			message_offset = this->get_message_offset(read_batch.get(), batch_size, message_id, &message_found, true);

			if (message_id == 0) {
				message_found = true;
				message_offset = 0;
			}

			if (message_found) {
				unsigned int message_bytes = 0;
				unsigned int changed = 0;
				bool is_message_active = true;
				unsigned long long current_message_id = 0;

				unsigned int offset = message_offset;

				while (offset <= batch_size - MESSAGE_TOTAL_BYTES) {
					memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
					memcpy_s(&is_message_active, MESSAGE_IS_ACTIVE_SIZE, read_batch.get() + offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE);
					memcpy_s(&current_message_id, MESSAGE_ID_SIZE, read_batch.get() + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

					if (offset + message_bytes <= batch_size && !Helper::has_valid_checksum(read_batch.get() + offset))
						throw CorruptionException("Corrupted message detected");

					if (is_message_active && current_message_id > message_id) {
						is_message_active = false;
						memcpy_s(read_batch.get() + offset + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE, &is_message_active, MESSAGE_IS_ACTIVE_SIZE);
						Helper::update_checksum(read_batch.get() + offset, message_bytes);
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

			if (batch_size < READ_MESSAGES_BATCH_SIZE) {
				this->lock_manager->release_segment_lock(partition, segment_to_read.get());
				segment_to_read = this->get_next_segment_to_remove_messages(partition, segment_to_read.get()->get_id());
				continue;
			}
		}

		if(segment_to_read != nullptr)
			this->lock_manager->release_segment_lock(partition, segment_to_read.get(), true);

		return true;
	}
	catch (const CorruptionException& ex)
	{
		this->logger->log_error(ex.what());

		if (segment_to_read != nullptr)
			this->lock_manager->release_segment_lock(partition, segment_to_read.get(), true);

		return false;
	}
	catch (const std::exception& ex)
	{
		this->logger->log_error(ex.what());

		if (segment_to_read != nullptr)
			this->lock_manager->release_segment_lock(partition, segment_to_read.get(), true);

		return false;
	}
}

std::shared_ptr<PartitionSegment> MessagesHandler::get_next_segment_to_remove_messages(Partition* partition, unsigned long long current_segment_id) {
	unsigned long long next_segment_id = current_segment_id + 1;

	bool is_internal_queue = Helper::is_internal_queue(partition->get_queue_name());

	std::string segment_key = this->pm->get_file_key(
		partition->get_queue_name(),
		next_segment_id,
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	std::string segment_path = this->pm->get_file_path(
		partition->get_queue_name(),
		next_segment_id,
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	std::shared_ptr<PartitionSegment> next_segment = std::shared_ptr<PartitionSegment>(
		new PartitionSegment(next_segment_id, segment_key, segment_path)
	);

	this->lock_manager->lock_segment(partition, next_segment.get(), true);

	if (!this->disk_flusher->path_exists(segment_path)) {
		this->lock_manager->release_segment_lock(partition, next_segment.get(), true);
		return nullptr;
	}

	return next_segment;
}