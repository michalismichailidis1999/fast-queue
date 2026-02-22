#include "../../header_files/queue_management/TransactionLockManager.h"

TransactionLockManager::TransactionLockManager() {}

void TransactionLockManager::lock_transaction(const std::string& tx_key) {
	std::unique_lock<std::mutex> map_lock(this->mut);

	if (this->locks.find(tx_key) == this->locks.end()) {
		this->locks[tx_key] = std::make_shared<tx_lock>();
		this->locks[tx_key].get()->references = 1;
	}
	else this->locks[tx_key].get()->references++;

	auto& s_lock = this->locks[tx_key];
	map_lock.unlock();

	s_lock.get()->mut.lock();
}

void TransactionLockManager::release_transaction_lock(const std::string& tx_key) {
	std::unique_lock<std::mutex> map_lock(this->mut);

	if (this->locks.find(tx_key) == this->locks.end()) return;

	auto& s_lock = this->locks[tx_key];
	map_lock.unlock();

	s_lock.get()->mut.unlock();

	map_lock.lock();

	if ((--s_lock.get()->references) <= 0) {
		this->locks.erase(tx_key);
		return;
	}
}