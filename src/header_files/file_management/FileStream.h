#pragma once
#include <string>
#include <mutex>

class FileStream {
private:
	std::string file_path;
	long end_pos;
	int fd;
	FILE* file;

	std::mutex mut;
public:
	FileStream();

	void set_file(const std::string& file_path, FILE* file);

	friend class FileHandler;
};