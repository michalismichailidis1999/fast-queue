#pragma once
#include "../Settings.h"
#include "../Enums.h"
#include "./RequestMapper.h"
#include "./Requests.h"
#include "./ClientRequestExecutor.h"
#include "./InternalRequestExecutor.h"
#include "../network_management/ConnectionsManager.h"

#include "../__linux/memcpy_s.h"

class RequestManager {
private:
	ConnectionsManager* cm;
	RequestMapper* mapper;
	ClientRequestExecutor* client_request_executor;
	InternalRequestExecutor* internal_request_executor;
	Settings* settings;
	Logger* logger;

	std::unique_ptr<char> ping_res;
	unsigned int ping_res_buff_size;

	bool is_user_authorized_for_action(AuthRequest* request);

	bool is_invalid_external_request(RequestType req_type);

	bool is_invalid_internal_request(RequestType req_type);
public:
	RequestManager(ConnectionsManager* cm, Settings* settings, ClientRequestExecutor* client_request_executor, InternalRequestExecutor* internal_request_executor, RequestMapper* mapper, Logger* logger);

	void execute_request(SOCKET_ID socket, SSL* ssl, bool internal_communication);
};
