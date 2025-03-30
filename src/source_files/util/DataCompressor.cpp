#include "../../header_files/util/DataCompressor.h"

DataCompressor::DataCompressor(CompressionAlgorithm algorithm) {
	this->algorithm = algorithm;
}

std::shared_ptr<unsigned char> DataCompressor::compress_data(void* data) {
	switch (this->algorithm)
	{
		case CompressionAlgorithm::LZ4:
			return std::shared_ptr<unsigned char>((unsigned char*)data);
		default:
			return nullptr;
	}
}