#include "../header_files/SettingsUpdateHandler.h"

SettingsUpdateHandler::SettingsUpdateHandler(const std::string& settings_path, FileHandler* fh, Settings* settings, Logger* logger) {
	this->settings_path = settings_path;
	this->fh = fh;
	this->settings = settings;
	this->logger = logger;
}

void SettingsUpdateHandler::check_if_settings_updated(std::atomic_bool* should_terminate) {
	while (!(*should_terminate)) {
		if (!this->fh->check_if_exists(this->settings_path)) {
			this->logger->log_error("Could not find settings configuration at path: " + this->settings_path);
			std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_SETTINGS_UPDATE));
		}
		std::tuple<long, std::shared_ptr<char>> config_res = this->fh->get_complete_file_content(this->settings_path);

		std::unique_ptr<Settings> new_settings = std::unique_ptr<Settings>(new Settings(std::get<1>(config_res).get(), std::get<0>(config_res)));

		if (this->settings->should_stop_server(new_settings.get())) {
			this->logger->log_error("New settings updates triggered server shutdown");
			exit(EXIT_FAILURE);
		}

		this->settings->update_values_with_new_settings(new_settings.get());

		std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_SETTINGS_UPDATE));
	}
}