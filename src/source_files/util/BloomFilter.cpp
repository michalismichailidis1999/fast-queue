#include "../../header_files/util/BloomFilter.h"

BloomFilter::BloomFilter(unsigned int size) {
	this->size = size / sizeof(uint8_t);
	this->bit_arr = std::vector<uint8_t>(this->size, 0);
    this->seed = 0;
}

void BloomFilter::add(void* key, unsigned int key_bytes) {
    unsigned int pos = this->murmur3_32(key, key_bytes, this->seed);

    unsigned int index = pos / this->size;
    uint8_t byte_pos = pos % sizeof(uint8_t);

    this->bit_arr[index] |= byte_pos;
}

bool BloomFilter::has(void* key, unsigned int key_bytes) {
    unsigned int pos = this->murmur3_32(key, key_bytes, this->seed);

    unsigned int index = pos / this->size;
    uint8_t byte_pos = pos % sizeof(uint8_t);

	return this->bit_arr[index] & byte_pos;
}

void BloomFilter::reset() {
	for (unsigned int i = 0; i < this->size; i++)
		this->bit_arr[i] = 0;
}

unsigned int BloomFilter::murmur3_32(void* key, unsigned int key_bytes, unsigned int seed) {
    unsigned int h = seed;
    const unsigned int c1 = 0xcc9e2d51;
    const unsigned int c2 = 0x1b873593;

    unsigned int i = 0;
    while (i + 4 <= key_bytes) {
        unsigned int k;
        memcpy_s(&k, sizeof(unsigned int), (char*)key + i, sizeof(unsigned int));
        i += 4;

        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;

        h ^= k;
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64;
    }

    unsigned int k1 = 0;
    switch (key_bytes & 3) {
    case 3: k1 ^= *((uint8_t*)((char*)key + i + 2)) << 16;
    case 2: k1 ^= *((uint8_t*)((char*)key + i + 1)) << 8;
    case 1: k1 ^= *((uint8_t*)((char*)key + i));
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;
        h ^= k1;
    }

    h ^= key_bytes;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}