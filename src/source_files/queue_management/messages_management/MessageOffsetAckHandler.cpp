#include "../../../header_files/queue_management/messages_management/MessageOffsetAckHandler.h"

MessageOffsetAckHandler::MessageOffsetAckHandler(FileHandler* fh, QueueSegmentFilePathMapper* pm) {
	this->fh = fh;
	this->pm = pm;
}

void MessageOffsetAckHandler::flush_consumer_offset_ack(Partition* partition, Consumer* consumer, unsigned long long offset) {
	std::lock_guard<std::shared_mutex> lock(partition->consumers_mut);

	consumer->set_offset(offset);

	std::unique_ptr<char> message_ack = std::unique_ptr<char>(new char[CONSUMER_ACK_TOTAL_BYTES]);

	int group_id_size = consumer->get_group_id().size();
	unsigned long long consumer_id = consumer->get_id();

	memcpy_s(message_ack.get() + CONSUMER_GROUP_ID_LENGTH_OFFSET, CONSUMER_GROUP_ID_LENGTH_SIZE, &group_id_size, CONSUMER_GROUP_ID_LENGTH_SIZE);
	memcpy_s(message_ack.get() + CONSUMER_GROUP_ID_OFFSET, group_id_size, consumer->get_group_id().c_str(), group_id_size);
	memcpy_s(message_ack.get() + CONSUMER_ID_OFFSET, CONSUMER_ID_SIZE, &consumer_id, CONSUMER_ID_SIZE);
	memcpy_s(message_ack.get() + CONSUMER_MESSAGE_ACK_OFFSET, CONSUMER_MESSAGE_ACK_SIZE, &offset, CONSUMER_MESSAGE_ACK_SIZE);

	if (!this->fh->check_if_exists(partition->get_offsets_path())) {
		std::string temp_offsets = this->pm->get_partition_offsets_path(partition->get_queue_name(), partition->get_partition_id(), true);

		if (this->fh->check_if_exists(temp_offsets)) {
			this->fh->delete_dir_or_file(partition->get_offsets_path(), partition->get_offsets_key());
			this->fh->rename_file("", temp_offsets, partition->get_offsets_path());
		}
		else {
			unsigned long long last_replicated_offset = partition->get_last_replicated_offset();

			this->fh->create_new_file(
				partition->get_offsets_path(),
				sizeof(unsigned long long),
				&last_replicated_offset,
				partition->get_offsets_key(),
				true
			);
		}
	}

	long long written_pos = this->fh->write_to_file(
		partition->get_offsets_key(),
		partition->get_offsets_path(),
		CONSUMER_ACK_TOTAL_BYTES,
		-1,
		message_ack.get(),
		true
	);

	if (written_pos >= MAX_PARTITION_OFFSETS_SIZE) this->compact_partition_offsets(partition);
}

void MessageOffsetAckHandler::compact_partition_offsets(Partition* partition) {
	std::string temp_offsets = this->pm->get_partition_offsets_path(partition->get_queue_name(), partition->get_partition_id(), true);

	unsigned long long last_replicated_offset = partition->get_last_replicated_offset();

	this->fh->create_new_file(
		temp_offsets,
		sizeof(unsigned long long),
		&last_replicated_offset,
		"",
		true
	);

	std::unordered_map<unsigned long long, unsigned long long> consumer_offsets;

	unsigned int batch_size = CONSUMER_ACK_TOTAL_BYTES * (READ_MESSAGES_BATCH_SIZE / CONSUMER_ACK_TOTAL_BYTES - 1);
	std::unique_ptr<char> read_batch = std::unique_ptr<char>(new char[batch_size]);

	unsigned int read_pos = sizeof(unsigned long long);

	unsigned long long consumer_id = 0;
	unsigned long long message_offset = 0;

	while (true) {
		unsigned int bytes_read = this->fh->read_from_file(
			partition->get_offsets_key(),
			partition->get_offsets_path(),
			batch_size,
			read_pos,
			read_batch.get()
		);

		if (bytes_read == 0) break;

		unsigned int offset = 0;

		while (offset < bytes_read) {
			memcpy_s(&consumer_id, CONSUMER_ID_SIZE, read_batch.get() + offset + CONSUMER_ID_OFFSET, CONSUMER_ID_SIZE);
			memcpy_s(&message_offset, CONSUMER_MESSAGE_ACK_SIZE, read_batch.get() + offset + CONSUMER_MESSAGE_ACK_OFFSET, CONSUMER_MESSAGE_ACK_SIZE);

			consumer_offsets[consumer_id] = message_offset;

			offset += CONSUMER_ACK_TOTAL_BYTES;
		}

		if (bytes_read < batch_size) break;

		read_pos += bytes_read;
	}

	std::unique_ptr<char> message_ack = std::unique_ptr<char>(new char[CONSUMER_ACK_TOTAL_BYTES]);

	for (auto& iter : consumer_offsets) {
		std::shared_ptr<Consumer> consumer = partition->get_consumer_with_nolock(iter.first);

		if (consumer == nullptr) continue;

		int group_id_size = consumer->get_group_id().size();

		memcpy_s(message_ack.get() + CONSUMER_GROUP_ID_LENGTH_OFFSET, CONSUMER_GROUP_ID_LENGTH_SIZE, &group_id_size, CONSUMER_GROUP_ID_LENGTH_SIZE);
		memcpy_s(message_ack.get() + CONSUMER_GROUP_ID_OFFSET, group_id_size, consumer->get_group_id().c_str(), group_id_size);
		memcpy_s(message_ack.get() + CONSUMER_ID_OFFSET, CONSUMER_ID_SIZE, &iter.first, CONSUMER_ID_SIZE);
		memcpy_s(message_ack.get() + CONSUMER_MESSAGE_ACK_OFFSET, CONSUMER_MESSAGE_ACK_SIZE, &iter.second, CONSUMER_MESSAGE_ACK_SIZE);

		this->fh->write_to_file(
			"",
			temp_offsets,
			CONSUMER_ACK_TOTAL_BYTES,
			-1,
			message_ack.get(),
			true
		);
	}

	this->fh->delete_dir_or_file(partition->get_offsets_path(), partition->get_offsets_key());
	this->fh->rename_file("", temp_offsets, partition->get_offsets_path());
}

void MessageOffsetAckHandler::assign_latest_offset_to_partition_consumers(Partition* partition) {
	std::unordered_map<unsigned long long, unsigned long long> consumer_offsets;

	unsigned int batch_size = CONSUMER_ACK_TOTAL_BYTES * (READ_MESSAGES_BATCH_SIZE / CONSUMER_ACK_TOTAL_BYTES - 1);
	std::unique_ptr<char> read_batch = std::unique_ptr<char>(new char[batch_size]);

	unsigned int read_pos = sizeof(unsigned long long);

	unsigned long long consumer_id = 0;
	unsigned long long message_offset = 0;

	while (true) {
		unsigned int bytes_read = this->fh->read_from_file(
			partition->get_offsets_key(),
			partition->get_offsets_path(),
			batch_size,
			read_pos,
			read_batch.get()
		);

		if (bytes_read == 0) break;

		unsigned int offset = 0;

		while (offset < bytes_read) {
			memcpy_s(&consumer_id, CONSUMER_ID_SIZE, read_batch.get() + offset + CONSUMER_ID_OFFSET, CONSUMER_ID_SIZE);
			memcpy_s(&message_offset, CONSUMER_MESSAGE_ACK_SIZE, read_batch.get() + offset + CONSUMER_MESSAGE_ACK_OFFSET, CONSUMER_MESSAGE_ACK_SIZE);

			consumer_offsets[consumer_id] = message_offset;

			offset += CONSUMER_ACK_TOTAL_BYTES;
		}

		if (bytes_read < batch_size) break;

		read_pos += bytes_read;
	}

	for (auto& iter : consumer_offsets) {
		std::shared_ptr<Consumer> consumer = partition->get_consumer(iter.first);

		if (consumer == nullptr) continue;

		consumer.get()->set_offset(iter.second);
	}
}