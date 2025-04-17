#include "../../header_files/file_management/FileStream.h"

FileStream::FileStream() {
	this->end_pos = 0;
	this->file = NULL;
	this->fd = -1;
	this->file_path = "";
}

void FileStream::set_file(const std::string& file_path, FILE* file) {
	this->file_path = file_path;
	this->file = file;

	this->fd = fileno(file);

	fseek(file, 0, SEEK_END);
	this->end_pos = ftell(file);
}