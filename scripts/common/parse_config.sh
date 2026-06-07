#!/bin/bash

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

# split by comma into array of nodes
IFS=',' read -ra nodes <<< "${config[controller_nodes]}"

for node in "${nodes[@]}"; do
    # split node_id from rest using @
    node_id="${node%%@*}" # everything before first @
    addresses="${node##*@}" # everything after first @

    # split internal and external address using &
    #external_addr="${addresses%%&*}" # everything before &
    internal_addr="${addresses##*&}" # everything after &

    # split host and port using :
    #internal_host="${internal_addr%%:*}" # everything before :
    internal_port="${internal_addr##*:}" # everything after :

    #external_host="${external_addr%%:*}" # everything before :
    #external_port="${external_addr##*:}" # everything after :

    config["node-$node_id"]="$internal_port"
done