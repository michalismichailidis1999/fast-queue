#!/bin/bash

usage() {
cat << EOF
Usage: $(basename "$0") [OPTIONS]

Description:
  Print to screen cluster metadata changes.

Options:
  -h, --help              Show this help message and exit
  --config-map            *Configuration file path to load cluster node settings
  --queue                 *Queue to delete

Examples:
  $(basename "$0") --config-map /test_path/settings.conf --queue temp
EOF
exit 1
}