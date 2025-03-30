#pragma once
#include <string>
#include <vector>
#include <memory>
#include <map>
#include <functional>
#include <filesystem>
#include "../generic/Cache.h"
#include "./FileStream.h"
#include "../Settings.h"

class FileHandler {
private:
	Cache<std::string, std::shared_ptr<FileStream>>* cache;
	std::map<std::string, std::shared_ptr<FileStream>> static_files;

	void open_file(FileStream* fs, const std::string& path);
	void handle_file_failure(FileStream* fs);
public:
	FileHandler();

	void write_to_file(std::string file_key, const std::string& file_path, long buffer_size, long pos, void* data, bool is_static = false);
	void read_from_file(std::string file_key, const std::string& file_path, long buffer_size, long pos, void* dest, bool is_static = false);
	void clear_file_contents(std::string file_key, const std::string& file_path);

	bool check_if_exists(const std::string& path);
	bool create_directory(const std::string& path);
	void create_new_file(const std::string& path, long data_to_allocate, void* data_to_write = NULL, const std::string& key = "", bool is_static = false);
	void delete_dir_or_file(const std::string& path);
	void copy_all_path_directories(const std::string& path, std::vector<std::string>* list);

	void execute_action_to_dir_subfiles(const std::string& path, std::function<void(const std::filesystem::directory_entry&)> action);
	
	std::tuple<long, std::shared_ptr<char>> get_complete_file_content(const std::string& file_path);
};