#include "../../header_files/queue_management/CompactionHandler.h"

CompactionHandler::CompactionHandler(QueueManager* qm, Logger* logger, Settings* settings) {
	this->qm = qm;
	this->logger = logger;
	this->settings = settings;
}

void CompactionHandler::compact_closed_segments(std::atomic_bool* should_terminate) {
	while (!(*should_terminate)) {
		// TODO: Add logic here
		std::this_thread::sleep_for(std::chrono::milliseconds(5000));
	}
}