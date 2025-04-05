#include "../../../header_files/queue_management/messages_management/Consumer.h"

Consumer::Consumer() {
}

std::tuple<int, char*> Consumer::consume() {
	return std::tuple(0, nullptr);
}