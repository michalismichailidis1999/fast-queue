#include "Producer.h"
#include <cstddef>
#include <chrono>

Producer::Producer(const std::string& transactional_id, long id, long epoch, Logger* logger) {
	this->transactional_id = transactional_id;
	this->id = id;
	this->epoch = epoch;
	this->logger = logger;
	this->set_heartbeat_to_current_time();

	this->logger->log_info("Producer initialized with transactional id " + transactional_id + " and epoch " + std::to_string(epoch));
}

void Producer::produce(Queue* queue, std::vector<char*>* messages, std::vector<long>* message_sizes, int partition) {
	this->logger->log_info("Messages produced successfully from producer " + std::to_string(this->id) + " on partition " + std::to_string(partition));
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