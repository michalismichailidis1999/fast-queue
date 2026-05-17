#pragma once
#include <memory>
#include <atomic>
#include "./logging/Logger.h"

#include "./__linux/memcpy_s.h"

#if defined(_WIN32) || defined(_WIN64)

#include <windows.h>

#define PIPE_BASE_PATH "\\\\.\\pipe\\"
#define INVALID_PIPE_FD INVALID_HANDLE_VALUE

typedef HANDLE pipe_fd_t;

#else

#include <sys/stat.h>

#define PIPE_BASE_PATH "/tmp/"
#define INVALID_PIPE_FD -1

typedef int pipe_fd_t;

#endif

// For communication with bash scripts
class InterProcessCommunicator {
private:
	Logger* logger;

	std::shared_ptr<char> receive_buf;
	std::shared_ptr<char> respond_buf;

	unsigned int total_bytes_read;

	std::string read_pipe_path;
	pipe_fd_t read_pipe_fd;

	pipe_fd_t create_read_pipe();
	void init_pipe();
	int data_available();
	int read_from_pipe();
	void reset_read_pipe();
	int respond_to_process(const std::string& pipe_to_write);
	void cleanup_read_pipe();
	void cleanup_response_pipe(const std::string& pipe_to_write);
public:
	InterProcessCommunicator(Logger* logger);

	void start_ipc(std::atomic_bool *should_terminate);
};