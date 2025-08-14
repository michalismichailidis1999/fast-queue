#pragma once
#include <string>
#include <exception>

#include "../__linux/memcpy_s.h"

// TODO: Keep corruption information in this class to know which part of data to fix
class CorruptionException : public std::exception {
private:
	std::string message;
	bool stop_server_importance;

public:
	CorruptionException(const std::string& msg) : message(msg) {}

	const char* what() const noexcept override {
		return message.c_str();
	}
};