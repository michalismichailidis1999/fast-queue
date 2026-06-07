#!/bin/bash

# Default values
CONFIG_PATH=""
QUEUE=""
QUEUE_NAME_SIZE=0

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config-path)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --queue)
      is_valid_queue_name $2 "--queue"
      QUEUE="$2"
      QUEUE_NAME_SIZE=${#QUEUE}
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

source ./common/parse_config.sh