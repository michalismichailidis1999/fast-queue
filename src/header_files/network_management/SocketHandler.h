#pragma once
#if defined(_WIN32) || defined(_WIN64)
	#include <winsock2.h>  // For Winsock functions
	#include <ws2tcpip.h>  // For IPv6 support (if needed)

	#pragma comment(lib, "Ws2_32.lib")  // Link against the Winsock library

	typedef SOCKET SOCKET_ID;
	typedef WSAPOLLFD POLLED_FD;

	#define POLLIN_EVENT POLLIN
	#define POLLOUT_EVENT POLLOUT
	#define POLLERR_EVENT POLLERR
	#define POLLHUP_EVENT POLLHUP
	#define POLLNVAL_EVENT POLLNVAL

	constexpr int close_command = SD_SEND;
	constexpr int invalid_socket = INVALID_SOCKET;
	constexpr int poll_error = -1;
#else
	#include <sys/types.h>
	#include <sys/socket.h>
	#include <netinet/in.h>
	#include <arpa/inet.h>
	#include <unistd.h>
	#include <poll.h>
	#include <errno.h>

	typedef int SOCKET_ID;
	typedef struct pollfd POLLED_FD;

	#define POLLIN_EVENT POLLIN
	#define POLLOUT_EVENT POLLOUT
	#define POLLERR_EVENT POLLERR
	#define POLLHUP_EVENT POLLHUP
	#define POLLNVAL_EVENT POLLNVAL

	#define SOCKET_ERROR -1

	constexpr int close_command = SHUT_WR;
	constexpr int invalid_socket = -1;
	constexpr int poll_error = -1;
#endif

#include <atomic>
#include "./Connection.h"
#include "../Settings.h"
#include "../logging/Logger.h"

#include "../__linux/memcpy_s.h"

class SocketHandler {
private:
	Settings* settings;
	Logger* logger;

	void setup_socket_timeout(SOCKET_ID socket, long timeout_ms);

	SOCKET_ID get_socket(long timeout_ms = -1);
public:
	SocketHandler(Settings* settings, Logger* logger);

	SOCKET_ID get_listen_socket(bool internal_communication);
	SOCKET_ID get_connect_socket(ConnectionInfo* info);
	SOCKET_ID accept_connection(SOCKET_ID listen_socket);

	bool close_socket(SOCKET_ID socket);

	void socket_cleanup();

	int poll_events(std::vector<POLLED_FD>* fds);
	bool pollin_event_occur(POLLED_FD* fd);
	bool error_event_occur(POLLED_FD* fd);

	int respond_to_socket(SOCKET_ID socket, char* res_buf, unsigned int res_buf_len);
	int receive_socket_buffer(SOCKET_ID socket, char* res_buf, unsigned int res_buf_len);

	bool is_connection_broken(int response_code);
};