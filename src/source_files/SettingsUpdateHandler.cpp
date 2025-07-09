#include "../header_files/SettingsUpdateHandler.h"

SettingsUpdateHandler::SettingsUpdateHandler(ConnectionsManager* cm, const std::string& settings_path, FileHandler* fh, Settings* settings, Logger* logger) {
	this->cm = cm;
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

		try
		{
			std::tuple<long, std::shared_ptr<char>> config_res = this->fh->get_complete_file_content(this->settings_path);

			std::unique_ptr<Settings> new_settings = std::unique_ptr<Settings>(new Settings(std::get<1>(config_res).get(), std::get<0>(config_res)));

			if (this->settings->should_stop_server(new_settings.get())) {
				this->logger->log_error("New settings updates triggered server shutdown");
				exit(EXIT_FAILURE);
			}

			std::unordered_map<int, std::shared_ptr<ConnectionInfo>> new_controller_nodes;

			bool controller_nodes_updates = false;
			bool node_type_changed = this->settings->get_is_controller_node() != new_settings->get_is_controller_node();

			for (auto& con_node : new_settings.get()->get_controller_nodes())
				new_controller_nodes[std::get<0>(con_node)] = std::get<1>(con_node);

			for (auto& con_node : this->settings->get_controller_nodes())
				if (new_controller_nodes.find(std::get<0>(con_node)) == new_controller_nodes.end()) {
					controller_nodes_updates = true;
					break;
				}
				else {
					std::shared_ptr<ConnectionInfo> old_info = std::get<1>(con_node);
					std::shared_ptr<ConnectionInfo> new_info = new_controller_nodes[std::get<0>(con_node)];

					if (old_info.get()->address != new_info.get()->address || old_info.get()->port != new_info.get()->port) {
						controller_nodes_updates = true;
						break;
					}
				}

			new_controller_nodes.clear();

			if (controller_nodes_updates || node_type_changed) {
				// TODO: Send raft command that contains this nodes type & controller nodes connection info
			}

			/*
			if (controller_nodes_updates)
				for (auto& con_node : *(new_settings.get()->get_controller_nodes())) {
					int node_id = std::get<0>(con_node);
					std::shared_ptr<ConnectionInfo> info = std::get<1>(con_node);

					if (node_id == this->settings->get_node_id()) continue;

					this->cm->remove_data_node_connections(node_id);
					this->cm->remove_controller_node_connections(node_id);
					this->cm->initialize_controller_node_connection_pool(node_id, info);
				}
			*/

			this->settings->update_values_with_new_settings(new_settings.get());
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while checking for settings updates. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_SETTINGS_UPDATE));
	}
}