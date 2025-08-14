#pragma once
#include "../util/Util.h"
#include "../util/Helper.h"
#include "../Settings.h"
#include "../Constants.h"

#include "../__linux/memcpy_s.h"

class QueueSegmentFilePathMapper {
private:
	Util* util;
	Settings* settings;

	std::string get_file_key(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file, bool compacted);

	std::string get_file_path(const std::string& queue_name, unsigned long long segment_id, int partition, bool index_file, bool compacted);
public:
	QueueSegmentFilePathMapper(Util* util, Settings* settings);

	std::string get_queue_folder_path(const std::string& queue_name);

	std::string get_partition_folder_key(const std::string& queue_name, int partition_id);

	std::string get_partition_folder_path(const std::string& queue_name, int partition_id);

	std::string get_file_key(const std::string& queue_name, unsigned long long segment_id, int partition = -1, bool index_file = false);

	std::string get_file_path(const std::string& queue_name, unsigned long long segment_id, int partition = -1, bool index_file = false);

	std::string get_compacted_file_key(const std::string& queue_name, unsigned long long segment_id, int partition = -1, bool index_file = false);

	std::string get_compacted_file_path(const std::string& queue_name, unsigned long long segment_id, int partition = -1, bool index_file = false);

	std::string get_metadata_file_key(const std::string& queue_name);

	std::string get_metadata_file_path(const std::string& queue_name);

	std::string get_segment_message_map_key(const std::string& queue_name, int partition = -1);

	std::string get_segment_message_map_path(const std::string& queue_name, int partition = -1);

	std::string get_partition_offsets_path(const std::string& queue_name, int partition, bool temp_file = false);

	std::string get_partition_offsets_key(const std::string& queue_name, int partition);
};