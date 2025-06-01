#include "../../header_files/file_management/QueueSegmentFilePathMapper.h"

QueueSegmentFilePathMapper::QueueSegmentFilePathMapper(Util* util, Settings* settings) {
	this->util = util;
	this->settings = settings;

	this->cluster_metadata_compaction_key = "cm_comp";
	this->cluster_metadata_compaction_path = this->settings->get_log_path()
		+ "/"
		+ CLUSTER_METADATA_QUEUE_NAME
		+ "/snapshot"
		+ FILE_EXTENSION;
}

std::string QueueSegmentFilePathMapper::get_queue_folder_path(const std::string& queue_name) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name;
}

std::string QueueSegmentFilePathMapper::get_partition_folder_path(const std::string& queue_name, int partition_id) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ "/partition-"
		+ std::to_string(partition_id);
}

std::string QueueSegmentFilePathMapper::get_file_key(const std::string& queue_name, unsigned long long segment_id, bool index_file) {
	return this->get_file_key(queue_name, segment_id, index_file, false);
}

std::string QueueSegmentFilePathMapper::get_file_path(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file) {
	return this->get_file_path(queue_name, segment_id, partition, index_file, false);
}

std::string QueueSegmentFilePathMapper::get_compacted_file_key(const std::string& queue_name, unsigned long long segment_id, bool index_file) {
	return this->get_file_key(queue_name, segment_id, index_file, true);
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

const std::string& QueueSegmentFilePathMapper::get_cluster_metadata_compaction_key() {
	return this->cluster_metadata_compaction_key;
}

const std::string& QueueSegmentFilePathMapper::get_cluster_metadata_compaction_path() {
	return this->cluster_metadata_compaction_path;
}

std::string QueueSegmentFilePathMapper::get_segment_message_map_key(const std::string& queue_name, int partition) {
	return queue_name + "_mmap_" + (partition >= 0 ? ("partition-" + std::to_string(partition)) : "");
}

std::string QueueSegmentFilePathMapper::get_segment_message_map_path(const std::string& queue_name, int partition) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ (partition >= 0 ? ("partition-" + std::to_string(partition) + "/") : "")
		+ "messages_location_map"
		+ FILE_EXTENSION;
}

std::string QueueSegmentFilePathMapper::get_file_key(const std::string& queue_name, unsigned long long segment_id, bool index_file, bool compacted) {
	return queue_name + "_" + (index_file ? "i_" : "") + std::to_string(segment_id) + (compacted ? "_comp" : "");
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