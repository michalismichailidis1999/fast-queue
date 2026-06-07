#!/bin/bash

source ./common/validations/is_valid_queue_name.sh
source ./delete_queue/help.sh
source ./delete_queue/parse_config.sh
source ./common/cluster_info/retrieve_leader_info.sh

# Create Queue Request Schema
REQUEST_TYPE=2
QUEUE_NAME_REQ_VALUE=4
# ===========================

total_request_bytes=$(( 12 + $QUEUE_NAME_SIZE ))
request="\\i${total_request_bytes}|\\i${REQUEST_TYPE}|\\i${QUEUE_NAME_REQ_VALUE}|\\i${QUEUE_NAME_SIZE}|\\s${QUEUE}"

response=$(perl ./common/send_tcp_request.pl "127.0.0.1" $leader_port $request "./delete_queue/response_handler.pl")
status=$?

if [ $status -ne 0 ]; then
    echo "Delete queue request failed"
    exit 1
fi

if [ $response -eq 1 ]; then
    echo "Queue $QUEUE deleted successfully"
else
    echo "Queue $QUEUE does not exist"
fi