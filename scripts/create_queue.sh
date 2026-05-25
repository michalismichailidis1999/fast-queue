#!/bin/bash

response=$(perl ./common/send_tcp_request.pl "127.0.0.1" 29877 "hello world")
status=$?

if [ $status -ne 0 ]; then
    echo "Create queue request failed"
    exit 1
fi

echo "Got: $response"

# ./create_queue.sh