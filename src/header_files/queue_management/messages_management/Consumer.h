#pragma once
#include <string>
#include <mutex>

class Consumer {
private:
	std::string group_id;

	unsigned long long id;

	unsigned long long offset;

	std::mutex mut;
public:
	Consumer(std::string group_id, unsigned long long id, unsigned long long offset);

	unsigned long long get_offset();

	void set_offset(unsigned long long offset);
};