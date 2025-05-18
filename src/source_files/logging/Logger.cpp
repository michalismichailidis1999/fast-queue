#include "../../header_files/logging/Logger.h"

Logger::Logger(const std::string& source_name, FileHandler* fh, Util* util, Settings* settings) {
	this->source_name = source_name;
	this->fh = fh;
	this->util = util;
	this->settings = settings;

	this->source_path = settings->get_trace_log_path() + "/" + this->source_name + ".txt";

	fh->create_directory(settings->get_trace_log_path());
	
	if (!this->fh->check_if_exists(this->source_path)) {
		this->fh->create_new_file(this->source_path, 0, NULL, source_name, true);
		this->log_info("Initialize log file " + this->source_path + " for source " + this->source_name);
	}
}

void Logger::log(const std::string& trace_type, const std::string& message) {
	try
	{
		std::string formatted_message =
			this->util->get_current_time_in_str()
			+ " | " + trace_type
			+ " | " + message + "\n";

		printf("%s", formatted_message.c_str());

		/*
		this->fh->write_to_file(
			this->source_name, 
			this->source_path, 
			formatted_message.size(), 
			-1, 
			(void*)formatted_message.c_str(),
			true
		);
		*/
	}
	catch (const std::exception&)
	{
		printf("Error occurred while trying to log trace message.\n");
	}
}

void Logger::log_info(const std::string& message, bool immediate_flush) {
	this->log(this->get_log_trace_type_in_str(LogTraceType::INFO), message);
}

void Logger::log_warning(const std::string& message, bool immediate_flush) {
	this->log(this->get_log_trace_type_in_str(LogTraceType::WARN), message);
}

void Logger::log_error(const std::string& message, bool immediate_flush) {
	this->log(this->get_log_trace_type_in_str(LogTraceType::INFO), message);
}

std::string Logger::get_log_trace_type_in_str(LogTraceType trace_type) {
	switch (trace_type)
	{
	case LogTraceType::DEBUG:
		return "DEBUG";
	case LogTraceType::INFO:
		return "INFO";
	case LogTraceType::WARN:
		return "WARNING";
	case LogTraceType::ERR:
		return "ERROR";
	default:
		return "";
	}
}