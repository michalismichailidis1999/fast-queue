#pragma once
#include <cstdint> 
#include <vector> 

class BloomFilter {
private:
	unsigned int size;
	unsigned int seed;
	std::vector<uint8_t> bit_arr;

	unsigned int murmur3_32(void* key, unsigned int len, unsigned int seed = 0);
public:
	BloomFilter(unsigned int size); // size needs to be multiple of 8 (total bits of a byte)

	void add(void* key, unsigned int key_bytes);

	bool has(void* key, unsigned int key_bytes);

	void reset();
};