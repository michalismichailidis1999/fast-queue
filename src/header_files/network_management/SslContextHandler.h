#pragma once
#include <openssl/ssl.h>
#include <openssl/err.h>
//#include <openssl/applink.c>

#include <memory>

#include "../Settings.h"
#include "../logging/Logger.h"

class SslContextHandler {
private:
	Settings* settings;
	Logger* logger;

	static void SSL_CTX_deleter(SSL_CTX* ctx);
public:
	SslContextHandler(Settings* settings, Logger* logger);

	std::shared_ptr<SSL_CTX> create_ssl_context(bool internal_communication);

	SSL* wrap_connection_with_ssl(SSL_CTX* ctx, int fd);
	bool free_ssl(SSL* ssl);

	int respond_to_ssl(SSL* ssl, char* res_buf, long res_buf_len);
	int receive_ssl_buffer(SSL* ssl, char* res_buf, long res_buf_len);

	bool is_connection_broken(int response_code);

	void initialize_ssl();
	void cleanup_ssl();
};