#pragma once
#include <mutex>
#include <unordered_map>
#include <memory>

#include "../__linux/memcpy_s.h"

typedef struct {
	std::mutex mut;
	int references;
} tx_lock;

class TransactionLockManager {
private:
	std::unordered_map<std::string, std::shared_ptr<tx_lock>> locks;
	std::mutex mut;
public:
	TransactionLockManager();

	void lock_transaction(const std::string& tx_key);
	void release_transaction_lock(const std::string& tx_key);
};