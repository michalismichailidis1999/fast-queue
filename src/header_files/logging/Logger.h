#pragma once
#include <string>
#include "../file_management/FileHandler.h"
#include "../util/Util.h"
#include "../Settings.h"
#include "../Enums.h"

#include "../__linux/memcpy_s.h"

class Logger {
private:
	FileHandler* fh;
	Settings* settings;
	Util* util;

	std::string source_name;
	std::string source_path;

	bool write_to_file;

	void log(const std::string& trace_type, const std::string& message);

	std::string get_log_trace_type_in_str(LogTraceType trace_type);
public:
	Logger(const std::string& source_name, FileHandler* fh, Util* util, Settings* settings);
	
	void log_info(const std::string& message, bool immediate_flush = false);

	void log_warning(const std::string& message, bool immediate_flush = false);

	void log_error(const std::string& message, bool immediate_flush = false);
};