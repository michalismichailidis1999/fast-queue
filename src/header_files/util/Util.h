#pragma once
#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>  // For std::put_time
#include <sstream>  // For std::ostringstream

#if defined(_WIN32) || defined(_WIN64)

#define GMTIME(tm_ptr, time_ptr) gmtime_s((tm_ptr), (time_ptr))

#else

#define GMTIME(tm_ptr, time_ptr) gmtime_r((time_ptr), (tm_ptr))

#endif

#include "../__linux/memcpy_s.h"

class Util {
public:
	Util();

	std::string get_current_time_in_str();
	std::string left_padding(unsigned long long num, int width, char pad_with);
	std::chrono::milliseconds get_current_time_milli();
	bool has_timeframe_expired(std::chrono::milliseconds initial_frame, long long range);
	bool has_timeframe_expired(long long initial_milli, long long range);
};