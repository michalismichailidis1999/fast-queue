#!/bin/bash

# Default values
CONFIG_PATH=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config-path)
      CONFIG_PATH="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

declare -A config # create associative array

while IFS= read -r line; do
  # Remove inline comments
  line="${line%%#*}"

  # Skip empty lines
  [[ -z "$line" ]] && continue

  # Split key and value
  key="${line%%=*}"
  value="${line#*=}"
  value="${value//$'\r'/}"

  # Store in hashmap
  config["$key"]="$value"

done < $CONFIG_PATH

# D:/MessageBrokerHelperFiles/dummy.conf