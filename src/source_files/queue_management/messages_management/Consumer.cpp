#include "../../../header_files/queue_management/messages_management/Consumer.h"

Consumer::Consumer(std::string group_id, unsigned long long id, unsigned long long offset) {
	this->group_id = group_id;
	this->id = id;
	this->offset = offset;
}

unsigned long long Consumer::get_offset() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->offset;
}

void Consumer::set_offset(unsigned long long offset) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->offset = offset;
}