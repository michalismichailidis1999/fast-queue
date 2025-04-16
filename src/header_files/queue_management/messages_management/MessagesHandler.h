#pragma once
#include <memory>
#include "../../file_management/DiskFlusher.h"
#include "../../file_management/DiskReader.h"

class MessagesHandler {
private:
	DiskFlusher* disk_flusher;
	DiskReader* disk_reader;
	QueueSegmentFilePathMapper* pm;
public:
	MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm);

	void flush_messages_to_disk();

	std::shared_ptr<void> retrieve_messages_from_disk();
};