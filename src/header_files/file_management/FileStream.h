#pragma once
#include <string>
#include <mutex>

#if defined(_WIN32) || defined(_WIN64)

#define FILE_TELL _ftelli64
#define FILE_SEEK _fseeki64

#else

#define FILE_TELL ftello64
#define FILE_SEEK fseeko64

#endif

#include "../__linux/memcpy_s.h"

class FileStream {
private:
	std::string file_path;
	long long end_pos;
	int fd;
	FILE* file;
	bool file_closed;

	std::mutex mut;
public:
	FileStream();

	void set_file(const std::string& file_path, FILE* file);

	friend class FileHandler;
};