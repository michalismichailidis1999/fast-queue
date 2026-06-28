#!/bin/bash

source ./common/validations/is_valid_queue_name.sh
source ./create_queue/help.sh
source ./show_partitions_info/parse_config.sh
source ./common/cluster_info/retrieve_leader_info.sh

# Retrieve Queue Partitions Info Request Schema
REQUEST_TYPE=24

QUEUE_NAME_REQ_VALUE=4
# ===========================

total_request_bytes=$(( 12 + $QUEUE_NAME_SIZE ))
request="\\i${total_request_bytes}|\\i${REQUEST_TYPE}|\\i${QUEUE_NAME_REQ_VALUE}|\\i${QUEUE_NAME_SIZE}|\\s${QUEUE}"

response=$(perl ./common/send_tcp_request.pl "127.0.0.1" $leader_port $request "./show_partitions_info/response_handler.pl")
status=$?

if [ $status -ne 0 ]; then
    echo "Failed to retrieve queue $QUEUE partition info"
    exit 1
fi