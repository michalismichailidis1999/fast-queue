#include "../../header_files/file_management/FileStream.h"

FileStream::FileStream() {
	this->end_pos = 0;
	this->file = NULL;
	this->fd = -1;
	this->file_path = "";
	this->file_closed = true;
}

void FileStream::set_file(const std::string& file_path, FILE* file) {
	this->file_path = file_path;
	this->file = file;
	this->file_closed = false;

	this->fd = fileno(file);

	fseek(file, 0, SEEK_END);
	this->end_pos = _ftelli64(file);
}