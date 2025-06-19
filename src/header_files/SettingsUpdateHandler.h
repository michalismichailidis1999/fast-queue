#pragma once
#include <atomic>
#include <chrono>
#include <string>
#include "./Settings.h"
#include "./logging/Logger.h"
#include "./file_management/FileHandler.h"
#include "./network_management/Connection.h"
#include "./network_management/ConnectionsManager.h"
#include "./Constants.h"

class SettingsUpdateHandler {
private:
	ConnectionsManager* cm;
	FileHandler* fh;
	Settings* settings;
	Logger* logger;

	std::string settings_path;
public:
	SettingsUpdateHandler(ConnectionsManager* cm, const std::string& settings_path, FileHandler* fh, Settings* settings, Logger* logger);

	void check_if_settings_updated(std::atomic_bool* should_terminate);
};