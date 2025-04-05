#pragma once
#include <ctime>
#include <string>
#include <chrono>
#include "../../logging/Logger.h"

class Producer {
private:
	std::string transactional_id;

	long id;
	long epoch;

	std::time_t last_heartbeat;

	Logger* logger;
public:
	Producer(const std::string& transactional_id, long id, long epoch, Logger* logger);

	void set_heartbeat_to_current_time();

	void produce();

	long get_id();
	long get_epoch();
};