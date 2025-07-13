#pragma once
#include <tuple>
#include <memory>
#include "../Enums.h"
#include "./Requests.h"
#include "./Responses.h"

class ClassToByteTransformer {
public:
    ClassToByteTransformer();

    // Requests
    std::tuple<long, std::shared_ptr<char>> transform(AppendEntriesRequest* obj, bool used_as_response = false);

    std::tuple<long, std::shared_ptr<char>> transform(RequestVoteRequest* obj);

    std::tuple<long, std::shared_ptr<char>> transform(DataNodeHeartbeatRequest* obj);

    std::tuple<long, std::shared_ptr<char>> transform(GetClusterMetadataUpdateRequest* obj);
    // ------------------

    // Responses

    // -- Internal
    std::tuple<long, std::shared_ptr<char>> transform(AppendEntriesResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(RequestVoteResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(DataNodeHeartbeatResponse* obj);
    // --

    // -- External
    std::tuple<long, std::shared_ptr<char>> transform(CreateQueueResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(DeleteQueueResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(GetControllerConnectionInfoResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(GetLeaderIdResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(ProduceMessagesResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(GetQueuePartitionsInfoResponse* obj);
    // --

    // ------------------
};