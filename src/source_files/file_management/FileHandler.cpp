#include "../../header_files/file_management/FileHandler.h"

FileHandler::FileHandler() {
	this->cache = new Cache<std::string, std::shared_ptr<FileStream>>(MAXIMUM_OPEN_FILE_DESCRIPTORS, "", nullptr);
}

void FileHandler::create_new_file(const std::string& path, unsigned long bytes_to_write, void* data, const std::string& key, bool flush_data) {
	std::shared_ptr<FileStream> fs = std::shared_ptr<FileStream>(
		new FileStream(),
		[](FileStream* _fs) {
			if (!_fs->file_closed) {
				fclose(_fs->file);
				_fs->file_closed = true;
			}
		}
	);

	this->open_file(fs.get(), path, true);

	if (bytes_to_write > 0 && data != NULL)
		this->write_to_file(fs.get(), bytes_to_write, 0, data, flush_data);

	if (key == "") this->close_file(fs.get());
	else {
		std::shared_ptr<FileStream> old_fs = this->cache->put(key, fs);
		if (old_fs != nullptr) this->close_file(old_fs.get());
	}
}

bool FileHandler::check_if_exists(const std::string& path) {
	return std::filesystem::exists(path);
}

bool FileHandler::create_directory(const std::string& path) {
	if (this->check_if_exists(path)) return false;

	std::filesystem::create_directory(path);

	return true;
}

void FileHandler::delete_dir_or_file(const std::string& path, const std::string& key, const std::string& key_prefix) {
	if (!this->check_if_exists(path)) return;

	if (key != "") this->close_file(key);

	if (key_prefix != "") {
		std::unordered_set<std::string> matched_keys;

		auto comp = [&](const std::string& key_prefix, const std::string& file_key) {
			int i = 0;
			int match = 0;

			while (i < key_prefix.size() && i < file_key.size()) {
				if (key_prefix[i] != file_key[i]) break;
				match++;
				i++;
			}

			return match == key_prefix.size();
		};

		this->cache->find_matching_keys(key_prefix, comp, &matched_keys);

		if (matched_keys.size() > 0)
			for (auto& fkey : matched_keys)
				this->close_file(fkey);
	}

	std::filesystem::remove_all(path);
}

void FileHandler::execute_action_to_dir_subfiles(const std::string& path, std::function<void(const std::filesystem::directory_entry&)> action) {
	if (!this->check_if_exists(path)) return;

	for (const auto& entry : std::filesystem::directory_iterator(path))
		action(entry);
}

std::tuple<long, std::shared_ptr<char>> FileHandler::get_complete_file_content(const std::string& path) {
	std::shared_ptr<FileStream> fs = std::shared_ptr<FileStream>(
		new FileStream(),
		[](FileStream* _fs) { 
			if (!_fs->file_closed) {
				fclose(_fs->file);
				_fs->file_closed = true;
			}
		}
	);

	this->open_file(fs.get(), path);

	long file_size = fs.get()->end_pos;

	std::shared_ptr<char> content = std::shared_ptr<char>(new char[file_size]);

	this->read_from_file(fs.get(), file_size, 0, content.get());

	this->close_file(fs.get());

	return std::tuple<long, std::shared_ptr<char>>(file_size, content);
}

std::string FileHandler::get_dir_entry_path(std::filesystem::directory_entry dir_entry) {
	std::string str_path = dir_entry.path().u8string();

	for (int i = 0; i < str_path.size(); i++)
		if (str_path[i] == '\\')
			str_path[i] = '/';

	return str_path;
}

long long FileHandler::write_to_file(std::string key, const std::string& path, unsigned long buffer_size, long long pos, void* data, bool flush_data) {
	if (!this->check_if_exists(path)) {
		const std::string err_msg = "Invalid path " + path;
		printf("Tried to write to invalid path %s\n", path.c_str());
		throw std::runtime_error(err_msg.c_str());
	}

	std::shared_ptr<FileStream> fs = key == ""
		? nullptr
		: this->cache->get(key);

	if (fs == nullptr) {
		fs = std::shared_ptr<FileStream>(
			new FileStream(),
			[](FileStream* _fs) {
				if (!_fs->file_closed) {
					fclose(_fs->file);
					_fs->file_closed = true;
				}
			}
		);

		this->open_file(fs.get(), path);

		if (key != "") {
			std::shared_ptr<FileStream> old_fs = this->cache->put(key, fs);
			if (old_fs != nullptr) this->close_file(old_fs.get());
		}
	}

	return this->write_to_file(fs.get(), buffer_size, pos, data, flush_data);
}

unsigned long FileHandler::read_from_file(std::string key, const std::string& path, unsigned long buffer_size, long long pos, void* dest) {
	if (!this->check_if_exists(path)) {
		const std::string err_msg = "Invalid path " + path;
		printf("Tried to read from invalid path %s\n", path.c_str());
		throw std::runtime_error(err_msg.c_str());
	}

	std::shared_ptr<FileStream> fs = key == ""
		? nullptr
		: this->cache->get(key);

	if (fs == NULL) {
		fs = std::shared_ptr<FileStream>(
			new FileStream(),
			[](FileStream* _fs) {
				if (!_fs->file_closed) {
					fclose(_fs->file);
					_fs->file_closed = true;
				}
			}
		);

		this->open_file(fs.get(), path);

		if (key != "") {
			std::shared_ptr<FileStream> old_fs = this->cache->put(key, fs);
			if (old_fs != nullptr) this->close_file(old_fs.get());
		}
	}

	return this->read_from_file(fs.get(), buffer_size, pos, dest);
}

void FileHandler::flush_output_streams() {
	std::lock_guard<std::mutex> lock(this->unflushed_streams_mut);

	for (auto& stream_iter : this->unflushed_streams)
		fflush(stream_iter.second);

	this->unflushed_streams.clear();
}

long long  FileHandler::write_to_file(FileStream* fs, unsigned long buffer_size, long long pos, void* data, bool flush_data) {
	std::lock_guard<std::mutex> lock(fs->mut);

	if (buffer_size == 0 || data == NULL) return -1;

	long long prev_end_pos = fs->end_pos;

	if (pos == -1) FILE_SEEK(fs->file, 0, SEEK_END);
	else FILE_SEEK(fs->file, pos, SEEK_SET);

	fwrite((char*)data, sizeof(char), buffer_size, fs->file);

	if (pos == -1 || fs->end_pos - pos < buffer_size) fs->end_pos += pos == -1 ? buffer_size : buffer_size - (fs->end_pos - pos);

	if (flush_data) fflush(fs->file);

	this->handle_file_failure(fs);

	if (!flush_data) this->add_unflushed_stream(fs);

	return pos == -1 ? prev_end_pos : pos;
}

unsigned long FileHandler::read_from_file(FileStream* fs, unsigned long buffer_size, long long pos, void* dest) {
	std::lock_guard<std::mutex> lock(fs->mut);

	if (buffer_size == 0 || dest == NULL) return 0;

	FILE_SEEK(fs->file, pos, SEEK_SET);
	fread(dest, sizeof(char), buffer_size, fs->file);

	bool feof_occured = feof(fs->file) != 0;

	this->handle_file_failure(fs);

	return feof_occured ? fs->end_pos - pos : buffer_size;
}

void FileHandler::open_file(FileStream* fs, const std::string& path, bool is_new_file) {
	std::string mode = is_new_file ? "wb+" : "rb+";

	if (this->check_if_exists(path))
		mode = "rb+";

	FILE* file = fopen(path.c_str(), mode.c_str());

	if (file == nullptr)
		throw std::runtime_error("Could not open file");

	fs->set_file(path, file);

	this->handle_file_failure(fs);
}

void FileHandler::close_file(FileStream* fs) {
	std::lock_guard<std::mutex> lock(fs->mut);
	this->remove_unflushed_stream(fs);

	if (!fs->file_closed) {
		fclose(fs->file);
		fs->file_closed = true;
		fs->file = NULL;
		fs->fd = -1;
		fs->file_path = "";
	}
}

void FileHandler::handle_file_failure(FileStream* fs) {
	if (ferror(fs->file) == 0) return;

	if (feof(fs->file) != 0) {
		clearerr(fs->file);
		return;
	}

	int err = errno;

	switch (err) {
	case ENOENT:
		printf("No such file or directory\n");
		exit(1);
	case EACCES:
		printf("Permission denied\n");
		exit(1);
	case EIO:
		printf("I/O error\n");
		exit(1);
	case ENOSPC:
		printf("No space left on device\n");
		exit(1);
	case EBADF:
		printf("Bad file descriptor\n");
		exit(1);
	default:
		return;
	}
}

void FileHandler::add_unflushed_stream(FileStream* fs) {
	std::lock_guard<std::mutex> lock(this->unflushed_streams_mut);
	this->unflushed_streams[fs->fd] = fs->file;
}

void FileHandler::remove_unflushed_stream(FileStream* fs) {
	std::lock_guard<std::mutex> lock(this->unflushed_streams_mut);
	this->unflushed_streams.erase(fs->fd);
}

void FileHandler::close_file(const std::string& key) {
	std::shared_ptr<FileStream> fs = this->cache->get(key);

	if (fs == nullptr) return;

	this->cache->remove(key);

	this->close_file(fs.get());
}

void FileHandler::rename_file(const std::string& current_key, const std::string& current_name, const std::string& new_name) {
	if (!this->check_if_exists(current_name)) return;

	this->close_file(current_key);

	if (rename(current_name.c_str(), new_name.c_str()) != 0)
		throw std::runtime_error("Error renaming file");
}