#pragma once
#include <string>
#include <chrono>

class Util {
public:
	Util();

	std::string get_current_time_in_str();
	std::string left_padding(unsigned long long num, int width, char pad_with);
	std::chrono::milliseconds get_current_time_milli();
	bool has_timeframe_expired(std::chrono::milliseconds initial_frame, long long range);
};