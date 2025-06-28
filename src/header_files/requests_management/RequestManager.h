#pragma once
#include "../Settings.h"
#include "../Enums.h"
#include "./RequestMapper.h"
#include "./Requests.h"
#include "./ClientRequestExecutor.h"
#include "./InternalRequestExecutor.h"
#include "../network_management/ConnectionsManager.h"

class RequestManager {
private:
	ConnectionsManager* cm;
	RequestMapper* mapper;
	ClientRequestExecutor* client_request_executor;
	InternalRequestExecutor* internal_request_executor;
	Settings* settings;
	Logger* logger;

	bool is_user_authorized_for_action(char* username, int username_length, char* password, int password_length);
public:
	RequestManager(ConnectionsManager* cm, Settings* settings, ClientRequestExecutor* client_request_executor, InternalRequestExecutor* internal_request_executor, RequestMapper* mapper, Logger* logger);

	void execute_request(SOCKET_ID socket, SSL* ssl, bool internal_communication);
};
