#pragma once
#include "./SocketHandler.h"
#include "./SslContextHandler.h"
#include <string>

struct Connection {
	SOCKET_ID socket;
	SSL* ssl;
};

struct ConnectionInfo {
	std::string address;
	int port;
	std::string external_address;
	int external_port;
};