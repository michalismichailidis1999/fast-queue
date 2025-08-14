#pragma once
#include <memory>
#include "../Enums.h"

#include "../__linux/memcpy_s.h"

class DataCompressor {
private:
	CompressionAlgorithm algorithm;
public:
	DataCompressor(CompressionAlgorithm algorithm = CompressionAlgorithm::LZ4);

	std::shared_ptr<unsigned char> compress_data(void* data);
};