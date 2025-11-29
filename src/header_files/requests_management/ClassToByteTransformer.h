#pragma once
#include <tuple>
#include <string>
#include <memory>
#include "../Enums.h"
#include "./Requests.h"
#include "./Responses.h"

#include "../__linux/memcpy_s.h"

class ClassToByteTransformer {
public:
    ClassToByteTransformer();

    // Requests
    std::tuple<unsigned int, std::shared_ptr<char>> transform(AppendEntriesRequest* obj, bool used_as_response = false);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(RequestVoteRequest* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(DataNodeHeartbeatRequest* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(GetClusterMetadataUpdateRequest* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(ExpireConsumersRequest* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(AddLaggingFollowerRequest* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(RemoveLaggingFollowerRequest* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(FetchMessagesRequest* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(TransactionStatusUpdateRequest* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(UnregisterTransactionGroupRequest* obj);
    // ------------------

    // Responses

    // -- Internal
    std::tuple<unsigned int, std::shared_ptr<char>> transform(AppendEntriesResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(RequestVoteResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(DataNodeHeartbeatResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(ExpireConsumersResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(FetchMessagesResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(AddLaggingFollowerResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(RemoveLaggingFollowerResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(UnregisterTransactionGroupResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(TransactionStatusUpdateResponse* obj);
    // --

    // -- External
    std::tuple<unsigned int, std::shared_ptr<char>> transform(CreateQueueResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(DeleteQueueResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(GetControllerConnectionInfoResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(GetLeaderIdResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(ProduceMessagesResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(GetQueuePartitionsInfoResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(RegisterConsumerResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(GetConsumerAssignedPartitionsResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(ConsumeResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(AckMessageOffsetResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(RegisterTransactionGroupResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(BeginTransactionResponse* obj);

    std::tuple<unsigned int, std::shared_ptr<char>> transform(FinalizeTransactionResponse* obj);
    // --

    // ------------------
};