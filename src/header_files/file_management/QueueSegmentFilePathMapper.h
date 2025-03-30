#pragma once
#include "../util/Util.h"
#include "../Settings.h"
#include "../Constants.h"

class QueueSegmentFilePathMapper {
private:
	Util* util;
	Settings* settings;

public:
	QueueSegmentFilePathMapper(Util* util, Settings* settings);

	std::string get_file_key(const std::string& queue_name, unsigned long long segment_id);

	std::string get_file_path(const std::string& queue_name, unsigned long long segment_id, int partition = -1);

	std::string get_metadata_file_key(const std::string& queue_name);

	std::string get_metadata_file_path(const std::string& queue_name);
};