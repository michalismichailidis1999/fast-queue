#include "../../header_files/queue_management/QueueMetadata.h"

QueueMetadata::QueueMetadata(const std::string& name, unsigned int partitions, unsigned int replication_factor, CleanupPolicyType cleanup_policy) {
	this->name = name;
	this->partitions = partitions;
	this->replication_factor = replication_factor;
	this->status = Status::UNKNOWN;
	this->last_applied_index = 0;
	this->last_commit_index = 0;
	this->cleanup_policy = cleanup_policy;
}

QueueMetadata::QueueMetadata(void* metadata) {
	if (metadata == NULL)
		throw std::runtime_error("Queue metadata was NULL");

	char* queue_name = NULL;
	int queue_name_length = 0;

	memcpy_s(&queue_name_length, QUEUE_NAME_LENGTH_SIZE, (char*)metadata + QUEUE_NAME_LENGTH_OFFSET, QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(&this->partitions, QUEUE_PARTITIONS_SIZE, (char*)metadata + QUEUE_PARTITIONS_OFFSET, QUEUE_PARTITIONS_SIZE);
	memcpy_s(&this->replication_factor, QUEUE_REPLICATION_FACTOR_SIZE, (char*)metadata + QUEUE_REPLICATION_FACTOR_OFFSET, QUEUE_REPLICATION_FACTOR_SIZE);
	memcpy_s(&this->last_commit_index, QUEUE_LAST_COMMIT_INDEX_SIZE, (char*)metadata + QUEUE_LAST_COMMIT_INDEX_OFFSET, QUEUE_LAST_COMMIT_INDEX_SIZE);
	memcpy_s(&this->last_applied_index, QUEUE_LAST_APPLIED_INDEX_SIZE, (char*)metadata + QUEUE_LAST_APPLIED_INDEX_OFFSET, QUEUE_LAST_APPLIED_INDEX_SIZE);
	memcpy_s(&this->cleanup_policy, QUEUE_CLEANUP_POLICY_SIZE, (char*)metadata + QUEUE_CLEANUP_POLICY_OFFSET, QUEUE_CLEANUP_POLICY_SIZE);

	this->name = std::string((char*)metadata + QUEUE_NAME_OFFSET, queue_name_length);
	this->status = Status::ACTIVE;
}

const std::string& QueueMetadata::get_name() {
	return this->name;
}

unsigned int QueueMetadata::get_partitions() {
	return this->partitions;
}

unsigned QueueMetadata::get_replication_factor() {
	return this->replication_factor;
}

CleanupPolicyType QueueMetadata::get_cleanup_policy() {
	return this->cleanup_policy;
}

void QueueMetadata::set_status(Status status) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->status = status;
}

Status QueueMetadata::get_status() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->status;
}

unsigned long long QueueMetadata::get_last_commit_index() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->last_commit_index;
}

unsigned long long QueueMetadata::get_last_applied_index() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->last_applied_index;
}

std::tuple<int, std::shared_ptr<char>> QueueMetadata::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[QUEUE_METADATA_TOTAL_BYTES]);

	int queue_name_length = this->name.size();

	memcpy_s(bytes.get() + QUEUE_NAME_OFFSET, queue_name_length, this->name.c_str(), queue_name_length);
	memcpy_s(bytes.get() + QUEUE_NAME_LENGTH_OFFSET, QUEUE_NAME_LENGTH_SIZE, &queue_name_length, QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + QUEUE_PARTITIONS_OFFSET, QUEUE_PARTITIONS_SIZE, &this->partitions, QUEUE_PARTITIONS_SIZE);
	memcpy_s(bytes.get() + QUEUE_REPLICATION_FACTOR_OFFSET, QUEUE_REPLICATION_FACTOR_SIZE, &this->replication_factor, QUEUE_REPLICATION_FACTOR_SIZE);
	memcpy_s(bytes.get() + QUEUE_LAST_COMMIT_INDEX_OFFSET, QUEUE_LAST_COMMIT_INDEX_SIZE, &this->last_commit_index, QUEUE_LAST_COMMIT_INDEX_SIZE);
	memcpy_s(bytes.get() + QUEUE_LAST_APPLIED_INDEX_OFFSET, QUEUE_LAST_APPLIED_INDEX_SIZE, &this->last_applied_index, QUEUE_LAST_APPLIED_INDEX_SIZE);
	memcpy_s(bytes.get() + QUEUE_CLEANUP_POLICY_OFFSET, QUEUE_CLEANUP_POLICY_SIZE, &this->cleanup_policy, QUEUE_CLEANUP_POLICY_SIZE);

	Helper::add_common_metadata_values((void*)(bytes.get()), QUEUE_METADATA_TOTAL_BYTES);

	return std::tuple<int, std::shared_ptr<char>>(QUEUE_METADATA_TOTAL_BYTES, bytes);
}