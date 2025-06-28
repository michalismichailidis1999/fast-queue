#include "../../header_files/network_management/SocketHandler.h"

SocketHandler::SocketHandler(Settings* settings, Logger* logger) {
	this->settings = settings;
	this->logger = logger;
}

void SocketHandler::setup_socket_timeout(SOCKET_ID socket, long timeout_ms) {
    // Set socket options

    // TODOl Uncomment later
    //struct timeval timeout;
    //timeout.tv_sec = 0;
    //timeout.tv_usec = timeout_ms * 1000;
    //
    //setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
    //setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout));
}

SOCKET_ID SocketHandler::get_socket(long timeout_ms) {
    #ifdef _WIN32
        WSADATA wsaData;

        int websocket_res = WSAStartup(MAKEWORD(2, 2), &wsaData);

        if (websocket_res != 0) {
            this->logger->log_error("Failed to start Web Sockets");
            return invalid_socket;
        }
    #endif

    SOCKET_ID socket_id = invalid_socket;

    socket_id = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_id < 0) {
        this->logger->log_error("socket failed with error");
        this->socket_cleanup();
        return invalid_socket;
    }

    if(timeout_ms > 0)
        this->setup_socket_timeout(socket_id, timeout_ms);

    return socket_id;
}

SOCKET_ID SocketHandler::get_listen_socket(bool internal_communication) {
    SOCKET_ID listen_socket = this->get_socket(settings->get_request_timeout_ms());

    if (listen_socket == invalid_socket) return listen_socket;

    int port = internal_communication ? settings->get_internal_port() : settings->get_external_port();
    std::string ip = internal_communication ? settings->get_internal_ip() : settings->get_external_ip();

    sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons((u_short)port);

    if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
        this->logger->log_error("inet_pton failed with error");
        this->socket_cleanup();
        return invalid_socket;
    }

    if (bind(listen_socket, (sockaddr*)&serv_addr, sizeof(serv_addr)) == SOCKET_ERROR) {
        this->logger->log_error("bind failed with error");
        this->close_socket(listen_socket);
        this->socket_cleanup();
        return invalid_socket;
    }

    if (listen(listen_socket, SOMAXCONN) < 0) {
        this->logger->log_error("listen failed with error");
        this->close_socket(listen_socket);
        this->socket_cleanup();
        return invalid_socket;
    }

    std::string communication_info = internal_communication
        ? "for internal communication"
        : "for external communication";

    this->logger->log_info("Listening " + communication_info + " on port " + std::to_string(port));

    return listen_socket;
}

SOCKET_ID SocketHandler::get_connect_socket(ConnectionInfo* info) {
    SOCKET_ID connect_socket = this->get_socket(1000);

    sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons((u_short)(info->port));

    if (inet_pton(AF_INET, (info->address).c_str(), &serv_addr.sin_addr) <= 0) {
        this->logger->log_error("inet_pton failed with error");
        this->socket_cleanup();
        return invalid_socket;
    }

    if (connect(connect_socket, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        this->logger->log_error("Failed to connect to socket");
        this->socket_cleanup();
        return invalid_socket;
    }

    return connect_socket;
}

int SocketHandler::poll_events(std::vector<POLLED_FD>* fds) {
    #ifdef _WIN32
        return WSAPoll((*fds).data(), (*fds).size(), this->settings->get_request_polling_interval_ms());
    #else
        return poll((*fds).data(), (*fds).size(), this->settings->get_request_polling_interval_ms());
    #endif
}

bool SocketHandler::pollin_event_occur(POLLED_FD* fd) {
    return fd->revents & POLLIN_EVENT;
}

bool SocketHandler::error_event_occur(POLLED_FD* fd) {
    return fd->revents & (POLLERR_EVENT | POLLHUP_EVENT | POLLNVAL_EVENT);
}

bool SocketHandler::close_socket(SOCKET_ID socket) {
    try
    {
        #ifdef _WIN32
            if (closesocket(socket) < 0) return false;
        #else
            if (close(socket) < 0) return false;
        #endif

        return true;
    }
    catch (const std::exception&)
    {
        return false;
    }
}

void SocketHandler::socket_cleanup() {
	#ifdef _WIN32
		WSACleanup();
	#endif
}

SOCKET_ID SocketHandler::accept_connection(SOCKET_ID listen_socket) {
    return accept(listen_socket, NULL, NULL);
}

int SocketHandler::respond_to_socket(SOCKET_ID socket, char* res_buf, long res_buf_len) {
    return send(socket, res_buf, res_buf_len, 0);
}

int SocketHandler::receive_socket_buffer(SOCKET_ID socket, char* res_buf, long res_buf_len) {
    return recv(socket, res_buf, res_buf_len, 0);
}

bool SocketHandler::is_connection_broken(int response_code) {
    bool is_broken = response_code == ECONNRESET;

    #ifdef _WIN32
        is_broken = is_broken || response_code == WSAECONNRESET;
    #else
        is_broken = is_broken || response_code == EPIPE;
    #endif

    return is_broken;
}