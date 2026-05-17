#include "../header_files/InterProcessCommunicator.h"

InterProcessCommunicator::InterProcessCommunicator(Logger* logger) {
	this->logger = logger;

	this->receive_buf = std::make_shared<char>(IPC_REQUEST_BYTES); // 4KB
	this->respond_buf = std::make_shared<char>(IPC_RESPONSE_BYTES); // 4KB

	memset(this->receive_buf.get(), 0, IPC_REQUEST_BYTES);
	memset(this->respond_buf.get(), 0, IPC_RESPONSE_BYTES);

	this->read_pipe_path = PIPE_BASE_PATH + IPC_RECEIVER_PIPE_NAME;
	this->total_bytes_read = 0;
}

void InterProcessCommunicator::start_ipc(std::atomic_bool* should_terminate) {
	try {
		this->init_pipe();
		this->read_pipe_fd = this->create_read_pipe();
	}
	catch (const std::exception& ex) {
		std::string err_msg = "Failed to named pipe to read IPC requests. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
		should_terminate->store(true);
	}

	int has_data_to_read = -1;

	while (!should_terminate->load()) {
		try
		{
			has_data_to_read = this->data_available();

			if (has_data_to_read < 0) {
				this->reset_read_pipe();
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
				continue;
			}

			if (!has_data_to_read && total_bytes_read < IPC_REQUEST_BYTES) {
				std::this_thread::sleep_for(std::chrono::milliseconds(total_bytes_read == 0 ? 1000 : 50));
				continue;
			}

			if (this->read_from_pipe() <= 0) {
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
				continue;
			}

			if (total_bytes_read < IPC_REQUEST_BYTES) {
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
				continue;
			}

			int temp = 1;
			this->total_bytes_read = 0;
			memset(this->receive_buf.get(), 0, IPC_REQUEST_BYTES);
			memset(this->respond_buf.get(), 0, IPC_RESPONSE_BYTES);
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while trying to process bash script request. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
			this->total_bytes_read = 0;
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		}
	}

	this->cleanup_read_pipe();
}

pipe_fd_t InterProcessCommunicator::create_read_pipe() {
#if defined(_WIN32) || defined(_WIN64)
	HANDLE h = CreateNamedPipeA(
		this->read_pipe_path.c_str(),
		PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED, // async / non-blocking
		PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_NOWAIT, // non-blocking
		PIPE_UNLIMITED_INSTANCES,
		IPC_REQUEST_BYTES,
		IPC_REQUEST_BYTES,
		0,
		NULL
	);

	if (h == INVALID_PIPE_FD)
		throw std::runtime_error("Failed to create named pipe");

	return h;
#else
	int fd = open(this->read_pipe_path, O_RDONLY | O_NONBLOCK);

	if (fd == INVALID_PIPE_FD)
		throw std::runtime_error("Failed to create named pipe");

	return fd;
#endif
}

void InterProcessCommunicator::init_pipe() {
#if defined(_WIN32) || defined(_WIN64)
	// DO NOTHING
#else
	if (mkfifo(this->read_pipe_path, 0666) < 0)
		throw std::runtime_error("Failed to init read pipe");
#endif
}
int InterProcessCommunicator::data_available() {
#if defined(_WIN32) || defined(_WIN64)
	DWORD available = 0;
	if (!PeekNamedPipe(this->read_pipe_fd, NULL, 0, NULL, &available, NULL)) {
		DWORD err = GetLastError();
		
		if (err == ERROR_BROKEN_PIPE ||
			err == ERROR_NO_DATA ||
			err == ERROR_PIPE_NOT_CONNECTED
		) return -1;

		return 0;
	}
	return (int)available;
#else
	struct pollfd pfd;
	pfd.fd = fd;
	pfd.events = POLLIN;

	int ready = poll(&pfd, 1, 0); // timeout=0, don't wait

	if (ready < 0)  return -1;
	if (ready == 0) return  0;

	// POLLHUP means writer closed their end — but data may still be in the buffer
	if (pfd.revents & POLLIN)  return 1; // data ready
	if (pfd.revents & POLLHUP) return 0; // no data, reset

	return 0;
#endif
}

int InterProcessCommunicator::read_from_pipe() {
#if defined(_WIN32) || defined(_WIN64)
	DWORD bytes_read = 0;

    if (!ReadFile(this->read_pipe_fd, this->receive_buf.get() + this->total_bytes_read, IPC_REQUEST_BYTES, &bytes_read, NULL)) {
        DWORD err = GetLastError();

        if (err == ERROR_BROKEN_PIPE || err == ERROR_NO_DATA)
			return 0;

        return -1;
    }

	this->total_bytes_read += (unsigned int)bytes_read;

    return (int)bytes_read;
#else
	int bytes = (int)read(fd, this->receive_buf.get() + this->total_bytes_read, IPC_REQUEST_BYTES);

	if (bytes < 0 && errno == EAGAIN)
		return 0;

	this->total_bytes_read += (unsigned int)bytes;

	return bytes;
#endif
}

void InterProcessCommunicator::reset_read_pipe() {
#if defined(_WIN32) || defined(_WIN64)
	DisconnectNamedPipe(this->read_pipe_fd);
	ConnectNamedPipe(this->read_pipe_fd, NULL); // re-arm for next client
#else
	close(this->read_pipe_fd);
	this->read_pipe_fd = open(this->read_pipe_path, O_RDONLY | O_NONBLOCK);
#endif
}

void InterProcessCommunicator::cleanup_read_pipe() {
#if defined(_WIN32) || defined(_WIN64)
	DisconnectNamedPipe(this->read_pipe_fd);
	CloseHandle(this->read_pipe_fd);
#else
	close(this->read_pipe_fd);
	unlink(this->read_pipe_path);
#endif
}

int InterProcessCommunicator::respond_to_process(const std::string& pipe_to_write) {
	return -1;
}

void InterProcessCommunicator::cleanup_response_pipe(const std::string& pipe_to_write) {
#if defined(_WIN32) || defined(_WIN64)
	// DO NOTHING
#else
	unlink(pipe_to_write);
#endif
}