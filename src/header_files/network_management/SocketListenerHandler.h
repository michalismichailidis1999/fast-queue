#pragma once
#include "./ConnectionsManager.h"
#include "./SocketHandler.h"
#include "./SslContextHandler.h"
#include "../requests_management/RequestManager.h"
#include "../util/ThreadPool.h"
#include "../logging/Logger.h"
#include "../Settings.h"

class SocketListenerHandler {
private:
	ConnectionsManager* cm;
	SocketHandler* socket_handler;
	SslContextHandler* ssl_context_handler;
	RequestManager* rm;
	ThreadPool* thread_pool;
	Logger* logger;
	Settings* settings;

	std::atomic_int total_connections;

	std::atomic_bool* should_terminate;

public:
	SocketListenerHandler(ConnectionsManager* cm, SocketHandler* socket_handler, SslContextHandler* ssl_context_handler, RequestManager* rm, ThreadPool* thread_pool, Logger* logger, Settings* settings, std::atomic_bool* should_terminate);

	void create_and_run_socket_listener(bool internal_communication);
};