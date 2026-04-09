#!/bin/bash

# Default values
CONFIG_PATH=""
SEGMENT=""
OFFSET=0
MESSAGES_TO_PRINT=10

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config-path)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --segment)
      check_if_positive_integer $2 "--segment"
      SEGMENT=$2
      set_segment_file "$2"
      shift 2
      ;;
    --offset)
      check_if_positive_integer $2 "--offset"
      OFFSET=$2
      shift 2
      ;;
    --messages-to-print)
      check_if_positive_integer $2 "--messages-to-print"
      MESSAGES_TO_PRINT=$2
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