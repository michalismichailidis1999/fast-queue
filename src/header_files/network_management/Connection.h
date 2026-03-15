#pragma once
#include <string>
#include <memory>
#include "./SocketSession.h"

#include "../__linux/memcpy_s.h"

struct Connection {
	std::shared_ptr<SocketSession> socket_session;
	long long last_used_timestamp = 0;
};

struct ConnectionInfo {
	std::string address;
	int port;
	std::string external_address;
	int external_port;
};