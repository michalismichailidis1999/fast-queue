#!/bin/bash

source ./common/validations/is_number.sh
source ./common/validations/is_valid_queue_name.sh
source ./create_queue/help.sh
source ./create_queue/parse_config.sh
source ./common/cluster_info/retrieve_leader_info.sh

# Create Queue Request Schema
REQUEST_TYPE=1

QUEUE_NAME_REQ_VALUE=4
PARTITIONS_REQ_VALUE=5
REPLICATION_FACTOR_REQ_VALUE=6
CLEANUP_POLICY_REQ_VALUE=18
# ===========================

total_request_bytes=$(( 36 + $QUEUE_NAME_SIZE ))
request="\\i${total_request_bytes}|\\i${REQUEST_TYPE}|\\i${QUEUE_NAME_REQ_VALUE}|\\i${QUEUE_NAME_SIZE}|\\s${QUEUE}|\\i${PARTITIONS_REQ_VALUE}|\\i${PARTITIONS}|\\i${REPLICATION_FACTOR_REQ_VALUE}|\\i${REPLICATION_FACTOR}|\\i${CLEANUP_POLICY_REQ_VALUE}|\\i${COMPACT_SEGMENTS}"

response=$(perl ./common/send_tcp_request.pl "127.0.0.1" $leader_port $request "./create_queue/response_handler.pl")
status=$?

if [ $status -ne 0 ]; then
    echo "Create queue request failed"
    exit 1
fi

if [ $response -eq 1 ]; then
    echo "Queue $QUEUE created successfully"
else
    echo "Queue $QUEUE already exists"
fi