#pragma once
#include <cstdint> 
#include <vector>
#include <random>
#include "../Constants.h"

class BitArray {
private:
	unsigned int size;
	unsigned int seed;

	std::vector<uint8_t> bits;

	unsigned int murmur3_32(void* key, unsigned int len);
public:
	BitArray();
	BitArray(unsigned int size);

	void add(void* key, unsigned int key_bytes);

	bool has(void* key, unsigned int key_bytes);

	void reset();
};

class BloomFilter {
private:
	unsigned int hash_functions;
	std::vector<BitArray> arrays;
public:
	// size needs to be multiple of 8 (total bits of a byte) and divisible by hash_functions
	BloomFilter(unsigned int size, unsigned int hash_functions);

	void add(void* key, unsigned int key_bytes);

	bool has(void* key, unsigned int key_bytes);

	void reset();
};