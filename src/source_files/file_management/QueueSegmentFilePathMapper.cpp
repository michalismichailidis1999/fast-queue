#include "../../header_files/file_management/QueueSegmentFilePathMapper.h"

QueueSegmentFilePathMapper::QueueSegmentFilePathMapper(Util* util, Settings* settings) {
	this->util = util;
	this->settings = settings;
}

std::string QueueSegmentFilePathMapper::get_queue_folder_path(const std::string& queue_name) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name;
}

std::string QueueSegmentFilePathMapper::get_transactions_folder_path() {
	return this->settings->get_log_path() + "/" + TRANSACTIONS_QUEUE_NAME;
}

std::string QueueSegmentFilePathMapper::get_transactions_segment_path(unsigned int segment_id, bool temp_file) {
	return this->get_transactions_folder_path() + "/__segment_" + (temp_file ? "temp_" : "") + std::to_string(segment_id) + FILE_EXTENSION;
}

std::string QueueSegmentFilePathMapper::get_transactions_segment_key(unsigned int segment_id, bool temp_file) {
	return (temp_file ? "ts_t_" : "ts_") + std::to_string(segment_id);
}

std::string QueueSegmentFilePathMapper::get_partition_folder_key(const std::string& queue_name, int partition_id) {
	return queue_name + "_p_" + std::to_string(partition_id);
}

std::string QueueSegmentFilePathMapper::get_partition_folder_path(const std::string& queue_name, int partition_id) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ "/partition-"
		+ std::to_string(partition_id);
}

std::string QueueSegmentFilePathMapper::get_file_key(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file) {
	return this->get_file_key(queue_name, segment_id, partition, index_file, false);
}

std::string QueueSegmentFilePathMapper::get_file_path(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file) {
	return this->get_file_path(queue_name, segment_id, partition, index_file, false);
}

std::string QueueSegmentFilePathMapper::get_compacted_file_key(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file) {
	return this->get_file_key(queue_name, segment_id, partition, index_file, true);
}

std::string QueueSegmentFilePathMapper::get_compacted_file_path(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file) {
	return this->get_file_path(queue_name, segment_id, partition, index_file, true);
}

std::string QueueSegmentFilePathMapper::get_metadata_file_key(const std::string& queue_name) {
	return queue_name + "_m";
}

std::string QueueSegmentFilePathMapper::get_metadata_file_path(const std::string& queue_name) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ "/metadata"
		+ FILE_EXTENSION;
}

std::string QueueSegmentFilePathMapper::get_segment_message_map_key(const std::string& queue_name, int partition) {
	return queue_name + "_mmap_" + (partition >= 0 ? ("partition-" + std::to_string(partition)) : "");
}

std::string QueueSegmentFilePathMapper::get_segment_message_map_path(const std::string& queue_name, int partition) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ (partition >= 0 ? ("/partition-" + std::to_string(partition)) : "")
		+ "/messages_location_map"
		+ FILE_EXTENSION;
}

std::string QueueSegmentFilePathMapper::get_file_key(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file, bool compacted) {
	return queue_name + "_" + (partition >= 0 ? "p_" + std::to_string(partition) + "_" : "") + (index_file ? "i_" : "") + std::to_string(segment_id) + (compacted ? "_comp" : "");
}

std::string QueueSegmentFilePathMapper::get_file_path(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file, bool compacted) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ "/"
		+ (partition >= 0 ? ("partition-" + std::to_string(partition) + "/") : "")
		+ (index_file ? "index_" : "")
		+ this->util->left_padding(segment_id, 20, '0')
		+ (compacted ? "_compacted" : "")
		+ FILE_EXTENSION;
}

std::string QueueSegmentFilePathMapper::get_partition_offsets_path(const std::string& queue_name, int partition, bool temp_file) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ "/partition-" + std::to_string(partition) + "/__offsets"
		+ (temp_file ? "_temp" : "")
		+ FILE_EXTENSION;
}

std::string QueueSegmentFilePathMapper::get_partition_offsets_key(const std::string& queue_name, int partition, bool temp_file) {
	return queue_name + "_p_" + std::to_string(partition) + (temp_file ? "_t" : "") + "_off";
}