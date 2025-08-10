#include "../../../header_files/queue_management/messages_management/MessageOffsetAckHandler.h"

MessageOffsetAckHandler::MessageOffsetAckHandler(FileHandler* fh, QueueSegmentFilePathMapper* pm) {
	this->fh = fh;
	this->pm = pm;
}

void MessageOffsetAckHandler::flush_partition_consumer_offsets(Partition* partition) {
	// TODO: Complete this method
}