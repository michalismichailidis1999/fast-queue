#include "../../../header_files/queue_management/messages_management/Producer.h"

Producer::Producer(const std::string& transactional_id, long id, long epoch, Logger* logger) {
	this->transactional_id = transactional_id;
	this->id = id;
	this->epoch = epoch;
	this->logger = logger;
	this->set_heartbeat_to_current_time();

	this->logger->log_info("Producer initialized with transactional id " + transactional_id + " and epoch " + std::to_string(epoch));
}

void Producer::produce() {
}

long Producer::get_id() {
	return this->id;
}

long Producer::get_epoch() {
	return this->epoch;
}

void Producer::set_heartbeat_to_current_time() {
	auto now = std::chrono::system_clock::now();
	this->last_heartbeat = std::chrono::system_clock::to_time_t(now);
}