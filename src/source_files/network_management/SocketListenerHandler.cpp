#include "../../header_files/network_management/SocketListenerHandler.h"

SocketListenerHandler::SocketListenerHandler(ConnectionsManager* cm, SocketHandler* socket_handler, SslContextHandler* ssl_context_handler, RequestManager* rm, ThreadPool* thread_pool, Logger* logger, Settings* settings, std::atomic_bool* should_terminate) {
	this->cm = cm;
	this->socket_handler = socket_handler;
	this->ssl_context_handler = ssl_context_handler;
	this->rm = rm;
	this->thread_pool = thread_pool;
    this->logger = logger;
	this->settings = settings;

    this->total_connections = 0;
    this->should_terminate = should_terminate;
}

void SocketListenerHandler::create_and_run_socket_listener(bool internal_communication) {
    std::shared_ptr<SSL_CTX> ctx = nullptr;

    bool ssl_enabled = internal_communication
        ? this->settings->get_internal_ssl_enabled()
        : this->settings->get_external_ssl_enabled();

    if (ssl_enabled) {
        this->logger->log_info("Enabling SSL...");

        std::shared_ptr<SSL_CTX> ctx = ssl_context_handler->create_ssl_context(internal_communication);

        if (!ctx.get()) {
            this->logger->log_error("Unable to create SSL context");
            ERR_print_errors_fp(stderr);
            *should_terminate = true;
            return;
        }

        this->logger->log_info("SSL enabled");
    }

    SOCKET_ID listen_socket = this->socket_handler->get_listen_socket(internal_communication);

    if (listen_socket == invalid_socket) {
        *should_terminate = true;
        return;
    }

    {
        std::string communication_info = internal_communication ? "internal" : "external";

        this->logger->log_info("Server started and listening for " + communication_info + " connections");
    }

    std::vector<POLLED_FD> fds;
    fds.push_back({ listen_socket, POLLIN_EVENT, 0 });


    std::map<SOCKET_ID, SSL*> fds_ssls;

    if (ssl_enabled)
        fds_ssls[listen_socket] = NULL;

    try
    {
        while (!(should_terminate->load())) {
            int pollResult = this->socket_handler->poll_events(&fds);

            if (pollResult > 0)
                for (int i = 0; i < fds.size(); ++i) {
                    if (this->socket_handler->pollin_event_occur(&fds[i]) && !this->socket_handler->error_event_occur(&fds[i])) {
                        if (i == 0) {
                            // Handle new incoming connection
                            SOCKET_ID newConn = this->socket_handler->accept_connection(listen_socket);

                            if ((++this->total_connections) > this->settings->get_maximum_connections()) {
                                this->total_connections--;

                                if (!this->socket_handler->close_socket(fds[i].fd))
                                    this->logger->log_error("Socket closure failed with error");

                                this->logger->log_error("Could not accept any more connections.");
                                continue;
                            }

                            if (newConn != invalid_socket) {
                                SSL* ssl = NULL;

                                if (ssl_enabled) {
                                    ssl = this->ssl_context_handler->wrap_connection_with_ssl(ctx.get(), newConn);

                                    if (ssl == NULL) {
                                        this->socket_handler->close_socket(newConn);
                                        continue;
                                    }

                                    fds_ssls[newConn] = ssl;
                                }

                                fds.push_back({ newConn, POLLIN, 0 });
                                this->cm->initialize_connection_heartbeat(newConn, ssl);
                                this->logger->log_info("New connection accepted");
                            }
                            else
                                this->logger->log_error("Failed to accept new connection");

                            this->cm->remove_socket_lock(newConn);
                        }
                        else {
                            SOCKET_ID socket = fds[i].fd;

                            this->cm->update_socket_heartbeat(socket);

                            if (!this->cm->add_socket_lock(socket)) continue;

                            SSL* ssl = ssl_enabled ? fds_ssls[i] : NULL;

                            this->thread_pool->enqueue([&, socket, internal_communication]() {
                                try
                                {
                                    rm->execute_request(socket, ssl, internal_communication);
                                }
                                catch (const std::exception& ex)
                                {
                                    this->logger->log_error(ex.what());
                                }
                                });
                        }
                    }
                    else if (this->socket_handler->error_event_occur(&fds[i])) {
                        SOCKET_ID socket = fds[i].fd;
                        bool socket_expired = this->cm->socket_expired(socket);

                        this->cm->remove_socket_lock(socket);

                        if (!socket_expired && !this->socket_handler->close_socket(socket))
                            this->logger->log_error("Socket closure failed with error");

                        this->cm->remove_socket_connection_heartbeat(socket);

                        this->logger->log_error("Connection from socket " + std::to_string(socket) + " removed");

                        fds.erase(fds.begin() + i);
                        total_connections--;

                        if (i > 0 && ssl_enabled && fds_ssls.find(socket) != fds_ssls.end()) {
                            if (!socket_expired && !this->ssl_context_handler->free_ssl(fds_ssls[socket]))
                                this->logger->log_error("Failed to cleanup socket ssl");

                            fds_ssls.erase(socket);
                        }

                        if (i == 0) {
                            *should_terminate = true;
                            break;
                        }

                        --i;
                    }
                }
            else if (pollResult == poll_error) this->logger->log_error("Request polling failed");

            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
    }
    catch (const std::exception& ex)
    {
        *should_terminate = true;
        std::string err_msg = "Something went wrong while listening to socket connections. Reason: " + std::string(ex.what()) + ". Stopping server...";
        this->logger->log_error(err_msg);
    }

    if (fds.size() > 0)
        for (int i = 0; i < fds.size(); i++) {
            bool socket_expired = this->cm->socket_expired(fds[i].fd);

            if (!socket_expired && !this->socket_handler->close_socket(fds[i].fd))
                this->logger->log_error("Socket closure failed with error");

            if (i > 0 && ssl_enabled && !socket_expired)
                SSL_free(fds_ssls[i]);

            this->cm->remove_socket_connection_heartbeat(fds[i].fd);
        }

    this->socket_handler->socket_cleanup();

    this->thread_pool->stop_workers();
}