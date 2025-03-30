#pragma once
#include <string>
#include <fstream>
#include <mutex>

class FileStream : public std::fstream {
private:
	long buffer_size;
	std::string file_path;

	std::mutex mut;
public:
	FileStream(const std::string& file_path);

	void compute_buffer_size();

	friend class FileHandler;
};