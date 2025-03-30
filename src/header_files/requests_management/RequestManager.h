#pragma once
#include "Settings.h"
#include "RequestMapper.h"
#include "ClientRequestExecutor.h"
#include "InternalRequestExecutor.h"
#include "ConnectionsManager.h"
#include "Enums.h"

class RequestManager {
private:
	ConnectionsManager* cm;
	RequestMapper* mapper;
	ClientRequestExecutor* client_request_executor;
	InternalRequestExecutor* internal_request_executor;
	Settings* settings;
	Logger* logger;
public:
	RequestManager(ConnectionsManager* cm, Settings* settings, ClientRequestExecutor* client_request_executor, InternalRequestExecutor* internal_request_executor, RequestMapper* mapper, Logger* logger);

	void execute_request(SOCKET_ID socket, SSL* ssl, bool internal_communication);
};
