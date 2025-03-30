#include "QueueSegmentFilePathMapper.h"
#include "Constants.cpp"

QueueSegmentFilePathMapper::QueueSegmentFilePathMapper(Util* util, Settings* settings) {
	this->util = util;
	this->settings = settings;
}

std::string QueueSegmentFilePathMapper::get_file_key(const std::string& queue_name, unsigned long long segment_id) {
	return queue_name + "_" + std::to_string(segment_id);
}

std::string QueueSegmentFilePathMapper::get_file_path(const std::string& queue_name, unsigned long long segment_id, int partition) {
	return this->settings->get_log_path()
		+ "\\"
		+ queue_name
		+ "\\"
		+ (partition >= 0 ? ("partition-" + std::to_string(partition) + "\\") : "")
		+ this->util->left_padding(segment_id, 20, '0')
		+ FILE_EXTENSION;
}

std::string QueueSegmentFilePathMapper::get_metadata_file_key(const std::string& queue_name) {
	return queue_name + "_m";
}

std::string QueueSegmentFilePathMapper::get_metadata_file_path(const std::string& queue_name) {
	return this->settings->get_log_path()
		+ "\\"
		+ queue_name
		+ "\\metadata"
		+ FILE_EXTENSION;
}