#pragma once
#include <queue>
#include <mutex>
#include <memory>
#include "../network_management/Connection.h"

class ConnectionPool {
private:
	int total_connections;
	int borrowed_connections;

	std::queue<std::shared_ptr<Connection>> connections;
	std::shared_ptr<ConnectionInfo> connection_info;

	std::mutex mut;
public:
	ConnectionPool(int total_connections, std::shared_ptr<ConnectionInfo> connection_info);

	void add_connection(std::shared_ptr<Connection> connection, bool returning_borrowed_connection = false);
	std::shared_ptr<Connection> get_connection();

	int get_connections_missing_count();
	int get_total_connections();
	bool no_connections_left();

	ConnectionInfo* get_connection_info();
};