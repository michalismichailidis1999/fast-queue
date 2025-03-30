#include "FileHandler.h"

FileHandler::FileHandler() {
	this->cache = new Cache<std::string, std::shared_ptr<FileStream>>(500, "", NULL);
}

void FileHandler::open_file(FileStream* fs, const std::string& path) {
	if (fs->is_open()) return;

	fs->open(path, std::ios::out | std::ios::in | std::ios::binary);

	this->handle_file_failure(fs);
}

void FileHandler::create_new_file(const std::string& path, long data_to_allocate, void* data_to_write, const std::string& key, bool is_static) {
	std::shared_ptr<FileStream> fs = std::shared_ptr<FileStream>(new FileStream(path));

	fs.get()->open(path, std::ios::out);

	this->handle_file_failure(fs.get());

	if (data_to_allocate > 0) {
		if (data_to_write == NULL) {
			std::unique_ptr<char> empty_data = std::unique_ptr<char>(new char[data_to_allocate]);
			fs.get()->write(empty_data.get(), data_to_allocate);
		}
		else fs.get()->write((char*)data_to_write, data_to_allocate);

		this->handle_file_failure(fs.get());

		fs.get()->flush();

		this->handle_file_failure(fs.get());
	}

	if (key == "") {
		fs.get()->close();

		this->handle_file_failure(fs.get());
	}
	else if (is_static) this->static_files[key] = fs;
	else this->cache->put(key, fs);
}

void FileHandler::handle_file_failure(FileStream* fs) {
	if (fs->fail()) {
		// TODO: Throw exception
		throw std::exception("File error occured");
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

void FileHandler::delete_dir_or_file(const std::string& path) {
	if (!this->check_if_exists(path)) return;

	std::filesystem::remove_all(path);
}

void FileHandler::copy_all_path_directories(const std::string& path, std::vector<std::string>* list) {
	if (!this->check_if_exists(path)) return;

	for (const auto& entry : std::filesystem::directory_iterator(path))
		list->push_back(entry.path().u8string());
}

void FileHandler::execute_action_to_dir_subfiles(const std::string& path, std::function<void(const std::filesystem::directory_entry&)> action) {
	if (!this->check_if_exists(path)) return;

	for (const auto& entry : std::filesystem::directory_iterator(path))
		action(entry);
}

void FileHandler::write_to_file(std::string file_key, const std::string& file_path, long buffer_size, long pos, void* data, bool is_static) {
	FileStream* fs = !is_static ? this->cache->get(file_key).get() : this->static_files[file_key].get();

	if (fs == NULL) {
		std::shared_ptr<FileStream> new_fs = std::shared_ptr<FileStream>(new FileStream(file_path));
		this->open_file(new_fs.get(), file_path);
		fs = new_fs.get();

		if (!is_static) this->cache->put(file_key, new_fs, true);
		else this->static_files[file_key] = new_fs;
	}

	std::lock_guard<std::mutex> lock(fs->mut);

	if (!fs->is_open())
		this->open_file(fs, fs->file_path);

	if (buffer_size == 0 || data == NULL) return;

	if (pos == -1) fs->seekp(0, fs->end);
	else fs->seekp(pos, fs->beg);

	fs->write((char*)data, buffer_size);

	fs->flush();

	this->handle_file_failure(fs);

	fs->clear();

	fs->compute_buffer_size();
}

void FileHandler::read_from_file(std::string file_key, const std::string& file_path, long buffer_size, long pos, void* dest, bool is_static) {
	FileStream* fs = !is_static ? this->cache->get(file_key).get() : this->static_files[file_key].get();

	if (fs == NULL) {
		std::shared_ptr<FileStream> new_fs = std::shared_ptr<FileStream>(new FileStream(file_path));
		this->open_file(new_fs.get(), file_path);
		fs = new_fs.get();

		if (!is_static) this->cache->put(file_key, new_fs, true);
		else this->static_files[file_key] = new_fs;
	}

	std::lock_guard<std::mutex> lock(fs->mut);

	if (!fs->is_open())
		this->open_file(fs, fs->file_path);

	if (buffer_size == 0 || dest == NULL) return;

	fs->seekg(pos, fs->beg);
	fs->read((char*)dest, buffer_size);

	this->handle_file_failure(fs);

	fs->clear();
}

void FileHandler::clear_file_contents(std::string file_key, const std::string& file_path) {

}

std::tuple<long, std::shared_ptr<char>> FileHandler::get_complete_file_content(const std::string& file_path) {
	std::shared_ptr<FileStream> new_fs = std::shared_ptr<FileStream>(new FileStream(file_path));
	this->open_file(new_fs.get(), file_path);

	new_fs.get()->compute_buffer_size();

	std::shared_ptr<char> content = std::shared_ptr<char>(new char[new_fs.get()->buffer_size]);

	new_fs.get()->seekg(0, new_fs.get()->beg);
	new_fs.get()->read(content.get(), new_fs.get()->buffer_size);

	this->handle_file_failure(new_fs.get());

	new_fs.get()->clear();

	return std::tuple<long, std::shared_ptr<char>>(new_fs.get()->buffer_size, content);
}