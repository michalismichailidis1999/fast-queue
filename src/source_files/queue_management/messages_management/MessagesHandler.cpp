#include "../../../header_files/queue_management/messages_management/MessagesHandler.h"

MessagesHandler::MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
	this->pm = pm;
}

void MessagesHandler::flush_messages_to_disk() {

}

std::shared_ptr<void> MessagesHandler::retrieve_messages_from_disk() {
	return nullptr;
}