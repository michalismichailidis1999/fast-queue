#pragma once
#include <atomic>
#include <chrono>
#include <string>
#include "./Settings.h"
#include "./logging/Logger.h"
#include "./file_management/FileHandler.h"
#include "./Constants.h"

class SettingsUpdateHandler {
private:
	FileHandler* fh;
	Settings* settings;
	Logger* logger;

	std::string settings_path;
public:
	SettingsUpdateHandler(const std::string& settings_path, FileHandler* fh, Settings* settings, Logger* logger);

	void check_if_settings_updated(std::atomic_bool* should_terminate);
};