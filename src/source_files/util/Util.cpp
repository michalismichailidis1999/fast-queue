#include "Util.h"
#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>  // For std::put_time
#include <sstream>  // For std::ostringstream

Util::Util() {}

std::string Util::get_current_time_in_str() {
    // Get current time as a time_point
    auto now = std::chrono::system_clock::now();

    // Convert time_point to time_t, which represents calendar time
    std::time_t currentTime = std::chrono::system_clock::to_time_t(now);

    std::tm utc_tm;

    gmtime_s(&utc_tm , &currentTime);  // gmtime gives UTC

    // Create a string stream to format the time
    std::ostringstream oss;
    oss << std::put_time(&utc_tm, "%Y-%m-%d %H:%M:%S");  // Format: Year-Month-Day Hour:Minute:Second

    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    // Add milliseconds to the formatted time
    oss << '.' << std::setfill('0') << std::setw(3) << milliseconds.count();  // Add milliseconds with leading zeros

    return oss.str();  // Return the formatted time string
}

std::string Util::left_padding(unsigned long long num, int width, char pad_with) {
    std::string num_with_padding = std::string(width, pad_with);
    std::string num_str = std::to_string(num);

    for (int i = 0; i < num_str.size(); i++)
        num_with_padding[num_with_padding.size() - 1 - i] = num_str[num_str.size() - 1 - i];

    return num_with_padding;
}

std::chrono::milliseconds Util::get_current_time_milli() {
    // Get the current time point from the system clock
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();

    // Convert to duration since the epoch in milliseconds
    return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
}

bool Util::has_timeframe_expired(std::chrono::milliseconds initial_frame, long long range) {
    long long current_milli = this->get_current_time_milli().count();
    bool expired = current_milli > initial_frame.count() + range;
    return expired;
}