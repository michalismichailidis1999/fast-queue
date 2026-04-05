#pragma once
#include <functional>
#include <mutex>
#include <unordered_map>
#if defined(_WIN32) || defined(_WIN64)
#define WIN32_LEAN_AND_MEAN // Prevents windows.h from including old winsock.h
#endif
#include <hwloc.h>
#include "../requests_management/RequestManager.h"

#include "../__linux/memcpy_s.h"

class SocketListenerHandler {
private:
	ConnectionsManager* cm;
	SocketHandler* socket_handler;
	RequestManager* rm;
	Logger* logger;
	Settings* settings;
	Util* util;

	alignas(64) std::atomic_int total_connections;

	std::atomic_bool* should_terminate;

	int total_cores;

	std::function<void(SocketSession*, char*, int)> execute_request_fn;
	std::function<void()> reduce_external_connections_count;
	std::function<void()> ignore_connections_count;
	std::function<void(int, long long)> remove_stored_socket_from_cache;

	const std::string dummy_response_byte = "a";

	std::unordered_map<int, std::shared_ptr<boost::asio::io_context>> internal_contexts;
	std::unordered_map<int, std::shared_ptr<boost::asio::io_context>> external_contexts;

	std::unordered_map<int, std::shared_ptr<SocketSession>> open_sockets;
	std::mutex open_sockets_mut;

	int next_internal_worker;
	int next_external_worker;

	void accept_connection(tcp::acceptor* acceptor, bool internal_communication, int core_id);
	void check_for_stop_request(boost::asio::executor_work_guard<boost::asio::io_context::executor_type>* work_guard, boost::asio::steady_timer* check_timer);
	void handle_new_connected_socket(tcp::socket socket, bool internal_communication, int core_id);

	std::tuple<std::shared_ptr<boost::asio::io_context>, int> get_next_io_context(bool internal_communication, int core_id);

	std::shared_ptr<boost::asio::io_context> get_io_context_by_core_id(bool internal_communication, int core_id);

	void store_new_socket(std::shared_ptr<SocketSession> socket);
	void remove_stored_socket(int fd, long long creation_time);
public:
	SocketListenerHandler(ConnectionsManager* cm, SocketHandler* socket_handler, RequestManager* rm, Logger* logger, Settings* settings, Util* util, std::atomic_bool* should_terminate, int total_cores);

	void create_and_run_socket_listener(bool internal_communication, hwloc_topology_t topo, int core_id);
};