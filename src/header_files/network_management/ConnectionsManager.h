#pragma once
#include <map>
#include <set>
#include <mutex>
#include <shared_mutex>
#include <queue>
#include <tuple>
#include <functional>
#include <chrono>
#include <thread>
#include "../logging/Logger.h"
#include "./SocketHandler.h"
#include "../util/ConnectionPool.h"
#include "../requests_management/ResponseMapper.h"
#include "../requests_management/Responses.h"
#include "../Enums.h"
#include "../util/Util.h"

#include "../__linux/memcpy_s.h"

struct ConnectionInfo;
struct Connection;

class ConnectionsManager {
private:
	std::map<int, std::shared_ptr<ConnectionPool>> controller_node_connections;
	std::map<int, std::shared_ptr<ConnectionPool>> data_node_connections;

	std::map<int, std::tuple<bool, std::chrono::milliseconds, SocketSession*>> connections_heartbeats;
	std::mutex heartbeats_mut;

	SocketHandler* socket_handler;
	ResponseMapper* response_mapper;
	Util* util;
	Settings* settings;
	Logger* logger;

	std::shared_mutex controllers_mut;
	std::shared_mutex data_mut;

	std::atomic_bool* should_terminate;

	unsigned int ping_req_bytes_size;
	std::unique_ptr<char> ping_req;

	bool create_node_connection_pool(int node_id, ConnectionPool* pool, long milliseconds_to_wait = 1000);
	
	void add_connections_to_pools(std::shared_mutex* connections_mut, std::map<int, std::shared_ptr<ConnectionPool>>* connections);

	void ping_connection_pools(std::shared_mutex* connections_mut, std::map<int, std::shared_ptr<ConnectionPool>>* connections);

	bool setup_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info, std::shared_mutex* connections_mut, std::map<int, std::shared_ptr<ConnectionPool>>* connections);

	bool should_wait_for_response(RequestType request_type);
public:
	ConnectionsManager(SocketHandler* socket_handler, ResponseMapper* response_mapper, Util* util, Settings* settings, Logger* logger, std::atomic_bool* should_terminate);

	bool receive_socket_buffer(SocketSession* socket_session, char* res_buf, unsigned int res_buf_len);
	bool respond_to_socket(SocketSession* socket_session, char* res_buf, unsigned int res_buf_len, bool synchronously = true);
	bool respond_to_socket_with_error(SocketSession* socket_session, ErrorCode error_code, const std::string& error_message, bool synchronously = true);

	std::tuple<std::shared_ptr<char>, int, bool> send_request_to_socket(SocketSession* socket_session, char* buf, unsigned int buf_len, const std::string& internal_requets_type);
	std::tuple<std::shared_ptr<char>, int, bool> send_request_to_socket(ConnectionPool* pool, int retries, char* buf, unsigned int buf_len, const std::string& internal_requets_type);

	void initialize_controller_nodes_connections();
	void terminate_connections();

	void keep_pool_connections_to_maximum();
	void ping_pool_connections();

	std::shared_mutex* get_controller_node_connections_mut();
	std::shared_mutex* get_data_node_connections_mut();

	void update_socket_heartbeat(int socket_fd);

	void remove_data_node_connections(int node_id);

	bool add_connection_to_pool(ConnectionPool* pool);
	void close_connection_pool(ConnectionPool* pool);

	void initialize_connection_heartbeat(SocketSession* socket_session);
	bool socket_expired(int socket_fd);
	void remove_socket_connection_heartbeat(int socket_fd);

	void check_connections_heartbeats();

	bool initialize_data_node_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info);
	bool initialize_controller_node_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info);

	std::map<int, std::shared_ptr<ConnectionPool>>* get_controller_node_connections();

	std::shared_ptr<ConnectionPool> get_controller_node_connection(int node_id);

	std::shared_ptr<ConnectionPool> get_node_connection_pool(int node_id);

	void close_socket_connection(SocketSession* socket_session);
}; 