#include "../../header_files/file_management/DiskReader.h"

// TODO: Implement caching for index page retrievals

DiskReader::DiskReader(FileHandler* fh, CacheHandler* ch, Logger* logger, Settings* settings) {
	this->fh = fh;
	this->ch = ch;
	this->logger = logger;
	this->settings = settings;
}

std::shared_ptr<char> DiskReader::read_message_from_cache(Partition* partition, PartitionSegment* segment, unsigned long long message_id) {
	return this->ch->get_message(
		CacheHandler::get_message_cache_key(
			partition->get_queue_name(),
			partition->get_partition_id(),
			segment->get_id(),
			message_id
		)
	);
}

std::shared_ptr<char> DiskReader::read_index_page_from_cache(Partition* partition, PartitionSegment* segment, unsigned long long page_offset) {
	return this->ch->get_index_page(
		CacheHandler::get_index_page_cache_key(
			partition->get_queue_name(),
			partition->get_partition_id(),
			segment->get_id(),
			page_offset
		)
	);
}

unsigned long DiskReader::read_data_from_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos) {
	return this->fh->read_from_file(
		key,
		path,
		total_bytes,
		pos,
		data
	);
}