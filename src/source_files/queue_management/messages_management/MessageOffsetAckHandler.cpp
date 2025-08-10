#include "../../../header_files/queue_management/messages_management/MessageOffsetAckHandler.h"

MessageOffsetAckHandler::MessageOffsetAckHandler(FileHandler* fh, QueueSegmentFilePathMapper* pm) {
	this->fh = fh;
	this->pm = pm;
}

void MessageOffsetAckHandler::flush_partition_consumer_offsets(Partition* partition, bool from_server_startup) {
	std::vector<std::shared_ptr<Consumer>> partition_consumers = partition->get_all_consumers();

	unsigned int total_consumers = partition_consumers.size();

	if (total_consumers == 0) return;

	int total_consumer_offsets_bytes = CONSUMER_TOTAL_BYTES * total_consumers;
	int current_partition_consumer_offset_bytes = partition->get_consumer_offsets_flushed_bytes();

	if (current_partition_consumer_offset_bytes < total_consumer_offsets_bytes)
		partition->set_consumer_offsets_flushed_bytes(total_consumer_offsets_bytes);

	unsigned int buff_bytes = sizeof(unsigned int) + total_consumer_offsets_bytes;

	std::unique_ptr<char> consumer_offsets_bytes = std::unique_ptr<char>(new char[buff_bytes]);

	unsigned int offset = 0;

	memcpy_s(consumer_offsets_bytes.get() + offset, sizeof(unsigned int), &total_consumers, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	for (auto& consumer : partition_consumers) {
		unsigned int group_id_length = consumer.get()->get_group_id().size();
		unsigned long long consumer_id = consumer.get()->get_id();
		unsigned long long last_consumed_message_offset = consumer.get()->get_offset();

		memcpy_s(consumer_offsets_bytes.get() + offset + CONSUMER_GROUP_ID_LENGTH_OFFSET, CONSUMER_GROUP_ID_LENGTH_SIZE, &group_id_length, CONSUMER_GROUP_ID_LENGTH_SIZE);
		memcpy_s(consumer_offsets_bytes.get() + offset + CONSUMER_GROUP_ID_OFFSET, group_id_length, consumer.get()->get_group_id().c_str(), group_id_length);
		memcpy_s(consumer_offsets_bytes.get() + offset + CONSUMER_ID_OFFSET, CONSUMER_ID_SIZE, &consumer_id, CONSUMER_ID_SIZE);
		memcpy_s(consumer_offsets_bytes.get() + offset + CONSUMER_LAST_CONSUMED_MESSAGE_ID_OFFSET, CONSUMER_LAST_CONSUMED_MESSAGE_ID_SIZE, &last_consumed_message_offset, CONSUMER_LAST_CONSUMED_MESSAGE_ID_SIZE);

		offset += CONSUMER_TOTAL_BYTES;
	}

	// If after unregistering many consumers the bytes diff is larger than 16KB then rewrite the __offsets file of the partition
	if (Helper::abs(total_consumer_offsets_bytes - current_partition_consumer_offset_bytes) >= (from_server_startup ? 0 : CONSUMER_OFFSETS_REWRITE_BYTES_DIFF)) {
		std::string temp_offsets_path = this->pm->get_partition_offsets_path(partition->get_queue_name(), partition->get_partition_id(), true);

		unsigned long long last_replicated_offset = partition->get_last_replicated_offset();

		this->fh->create_new_file(
			temp_offsets_path,
			sizeof(unsigned long long),
			&last_replicated_offset,
			"",
			true
		);

		this->fh->write_to_file(
			"",
			temp_offsets_path,
			total_consumer_offsets_bytes,
			sizeof(unsigned long long),
			consumer_offsets_bytes.get(),
			true
		);

		this->fh->delete_dir_or_file(partition->get_offsets_path(), partition->get_offsets_key());
		this->fh->rename_file("", temp_offsets_path, partition->get_offsets_path());

		partition->set_consumer_offsets_flushed_bytes(total_consumer_offsets_bytes);

		return;
	}

	if (!this->fh->check_if_exists(partition->get_offsets_path())) {
		unsigned long long last_replicated_offset = partition->get_last_replicated_offset();

		this->fh->create_new_file(
			partition->get_offsets_path(),
			sizeof(unsigned long long),
			&last_replicated_offset,
			partition->get_offsets_key(),
			true
		);
	}

	this->fh->write_to_file(
		partition->get_offsets_key(),
		partition->get_offsets_path(),
		total_consumer_offsets_bytes,
		sizeof(unsigned long long),
		consumer_offsets_bytes.get(),
		true
	);
}