#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <functional>
#include <filesystem>
#include <mutex>
#include "../generic/Cache.h"
#include "./FileStream.h"
#include "../Settings.h"
#include "../Constants.h"

class FileHandler {
private:
	Cache<std::string, std::shared_ptr<FileStream>>* cache;

	std::unordered_map<int, FILE*> unflushed_streams;
	std::mutex unflushed_streams_mut;

	void open_file(FileStream* fs, const std::string& path, bool is_new_file = false);
	void close_file(FileStream* fs);
	void handle_file_failure(FileStream* fs);

	long long write_to_file(FileStream* fs, unsigned long buffer_size, long long pos, void* data, bool flush_data = false);
	unsigned long read_from_file(FileStream* fs, unsigned long buffer_size, long long pos, void* dest);

	void add_unflushed_stream(FileStream* fs);
	void remove_unflushed_stream(FileStream* fs);
public:
	FileHandler();

	long long write_to_file(std::string key, const std::string& path, unsigned long buffer_size, long long pos, void* data, bool flush_data = false);
	unsigned long read_from_file(std::string key, const std::string& path, unsigned long buffer_size, long long pos, void* dest);

	void create_new_file(const std::string& path, unsigned long bytes_to_write, void* data = NULL, const std::string& key = "", bool flush_data = false);

	void flush_output_streams();
	
	bool check_if_exists(const std::string& path);
	bool create_directory(const std::string& path);
	void delete_dir_or_file(const std::string& path, const std::string& key = "");

	void execute_action_to_dir_subfiles(const std::string& path, std::function<void(const std::filesystem::directory_entry&)> action);
	
	std::tuple<long, std::shared_ptr<char>> get_complete_file_content(const std::string& path);

	std::string get_dir_entry_path(std::filesystem::directory_entry dir_entry);

	void close_file(const std::string& key);
};