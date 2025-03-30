#include "../../header_files/file_management/FileStream.h"

FileStream::FileStream(const std::string& file_path) {
	this->file_path = file_path;
	this->buffer_size = 0;
}

void FileStream::compute_buffer_size() {
	this->seekg(0, this->end);
	this->buffer_size = this->tellg();
	this->clear();
}