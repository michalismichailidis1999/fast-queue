#include "ConnectionPool.h"
#include "Connection.cpp"

ConnectionPool::ConnectionPool(int total_connections, std::shared_ptr<ConnectionInfo> connection_info) {
	this->total_connections = total_connections;
	this->borrowed_connections = 0;
	this->connection_info = connection_info;
}

void ConnectionPool::add_connection(std::shared_ptr<Connection> connection, bool returning_borrowed_connection) {
	std::lock_guard<std::mutex> lock(this->mut);

	if (returning_borrowed_connection) this->borrowed_connections--;

	if (connection == nullptr) return;

	this->connections.push(connection);
}

std::shared_ptr<Connection> ConnectionPool::get_connection() {
	std::lock_guard<std::mutex> lock(this->mut);

	if (this->connections.empty()) return nullptr;

	std::shared_ptr<Connection> connection = this->connections.front();

	this->borrowed_connections++;
	this->connections.pop();

	return connection;
}

int ConnectionPool::get_connections_missing_count() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->total_connections - this->connections.size() - this->borrowed_connections;
}

int ConnectionPool::get_total_connections() {
	return this->total_connections;
}

bool ConnectionPool::no_connections_left() {
	std::lock_guard<std::mutex> lock(this->mut);
	return (this->connections.size() | this->borrowed_connections) == 0;
}

ConnectionInfo* ConnectionPool::get_connection_info() {
	return this->connection_info.get();
}