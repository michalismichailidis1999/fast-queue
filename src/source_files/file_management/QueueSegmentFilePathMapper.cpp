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

std::string QueueSegmentFilePathMapper::get_partition_folder_path(const std::string& queue_name, int partition_id) {
	if (Helper::is_internal_queue(queue_name)) return "";

	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ "/partition-"
		+ std::to_string(partition_id);
}

std::string QueueSegmentFilePathMapper::get_file_key(const std::string& queue_name, unsigned long long segment_id, bool index_file) {
	return queue_name + "_" + (index_file ? "i_" : "") + std::to_string(segment_id);
}

std::string QueueSegmentFilePathMapper::get_file_path(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file) {
	return this->settings->get_log_path()
		+ "/"
		+ queue_name
		+ "/"
		+ (partition >= 0 ? ("partition-" + std::to_string(partition) + "/") : "")
		+ (index_file ? "index_" : "")
		+ this->util->left_padding(segment_id, 20, '0')
		+ FILE_EXTENSION;
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