#include "../../../../header_files/queue_management/messages_management/index_management/BPlusTreeIndexHandler.h"

BPlusTreeIndexHandler::BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
}

void BPlusTreeIndexHandler::add_message_to_index(PartitionSegment* segment, unsigned long long message_id, unsigned int message_pos) {

}