#include "../../header_files/network_management/SslContextHandler.h"

SslContextHandler::SslContextHandler(Settings* settings, Logger* logger) {
	this->settings = settings;
	this->logger = logger;
}

void SslContextHandler::SSL_CTX_deleter(SSL_CTX* ctx) {
	if (ctx) SSL_CTX_free(ctx);
}

std::shared_ptr<SSL_CTX> SslContextHandler::create_ssl_context(bool internal_communication) {
	try
	{
        std::shared_ptr<SSL_CTX> ctx = std::shared_ptr<SSL_CTX>(SSL_CTX_new(SSLv23_server_method()), SSL_CTX_deleter);

        // Load server certificate and key
        if (
            SSL_CTX_use_certificate_file(
                ctx.get(),
                internal_communication
                ? settings->get_internal_ssl_cert_path().c_str()
                : settings->get_external_ssl_cert_path().c_str(),
                SSL_FILETYPE_PEM
            ) <= 0
            ) {
            ERR_print_errors_fp(stderr);
            return nullptr;
        }

        if (
            SSL_CTX_use_PrivateKey_file(
                ctx.get(),
                internal_communication
                ? settings->get_internal_ssl_cert_key_path().c_str()
                : settings->get_external_ssl_cert_key_path().c_str(),
                SSL_FILETYPE_PEM
        ) <= 0
            ) {
            ERR_print_errors_fp(stderr);
            return nullptr;
        }

        // Load the CA certificate for mTLS
        if (
            SSL_CTX_load_verify_locations(
                ctx.get(),
                internal_communication
                ? settings->get_internal_ssl_cert_ca_path().c_str()
                : settings->get_internal_ssl_cert_ca_path().c_str(),
                NULL
        ) <= 0
            ) {
            ERR_print_errors_fp(stderr);
            return nullptr;
        }

        bool mutual_tls_enabled = false; // TODO: Probably implement logic for this also

        if (mutual_tls_enabled) {
            // Enable client certificate verification for mTLS
            SSL_CTX_set_verify(ctx.get(), SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);
            SSL_CTX_set_verify_depth(ctx.get(), 1);
        }
        else
            SSL_CTX_set_verify(ctx.get(), SSL_VERIFY_NONE, NULL);  // Server does not verify the client certificate

        return ctx;
	}
	catch (const std::exception&)
	{
        return nullptr;
	}
}

SSL* SslContextHandler::wrap_connection_with_ssl(SSL_CTX* ctx, int fd) {
    try
    {
        SSL* ssl = SSL_new(ctx);
        SSL_set_fd(ssl, fd);

        if (SSL_accept(ssl) <= 0) {
            ERR_print_errors_fp(stderr);
            this->logger->log_error("Failed TLS handshake");

            SSL_free(ssl);
            return NULL;
        }

        return ssl;
    }
    catch (const std::exception& ex)
    {
        this->logger->log_error(ex.what());
        return NULL;
    }
}

bool SslContextHandler::free_ssl(SSL* ssl) {
    try
    {
        SSL_free(ssl);
        return true;
    }
    catch (const std::exception& ex)
    {
        this->logger->log_error(ex.what());
        return false;
    }
}

void SslContextHandler::initialize_ssl() {
	SSL_library_init();
	SSL_load_error_strings();
	OpenSSL_add_all_algorithms();
}

void SslContextHandler::cleanup_ssl() {
	EVP_cleanup();
}

bool SslContextHandler::respond_to_ssl(SSL* ssl, char* res_buf, long res_buf_len) {
    bool success = SSL_write(ssl, res_buf, res_buf_len) > 0;

    if(!success) ERR_print_errors_fp(stderr);

    return success;
}

bool SslContextHandler::receive_ssl_buffer(SSL* ssl, char* res_buf, long res_buf_len) {
    bool success = SSL_read(ssl, res_buf, res_buf_len) > 0;

    if (!success) ERR_print_errors_fp(stderr);

    return success;
}