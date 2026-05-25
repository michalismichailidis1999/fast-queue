#!/bin/bash

usage() {
cat << EOF
Usage: $(basename "$0") [OPTIONS]

Description:
  Print to screen cluster metadata changes.

Options:
  -h, --help              Show this help message and exit
  --config-map            *Configuration file path to load cluster node settings
  --queue                 *Queue to create
  --partitions            How many partitions the queue qill have (max = 1000) (default = 1)
  --replication-factor    How many replicas per partition (max = number of cluster nodes) (default = 1)
  --compact-segments      Bool value indicating if segment compaction should be enabled (by default retention policy is enabled) (default = 0)

Examples:
  $(basename "$0") --config-map /test_path/settings.conf --queue temp --partitions 1 --replication-factor 1 --compact-segments 1
EOF
exit 1
}