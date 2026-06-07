#!/bin/bash

# Create Queue Request Schema
REQUEST_TYPE=7
# ===========================

total_request_bytes=4
request="\\i${total_request_bytes}|\\i${REQUEST_TYPE}"

random_controller_node_port=0

cluster_node_key_prefix="node-"

for key in "${!config[@]}"; do
    if [[ "$key" == ${cluster_node_key_prefix}* ]]; then
        random_controller_node_port="${config[$key]}"
        break
    fi
done

leader_id=$(perl ./common/send_tcp_request.pl "127.0.0.1" $random_controller_node_port $request "./common/cluster_info/retrieve_leader_id_response_handler.pl")
status=$?

if [ $status -ne 0 ]; then
    echo "Failed to retrieve cluster leader id"
    exit 1
fi

if [[ ! -v config["node-$leader_id"] ]]; then
    echo "No controller node with id $leader_id found in configuration"
fi

leader_port="${config["node-$leader_id"]}"