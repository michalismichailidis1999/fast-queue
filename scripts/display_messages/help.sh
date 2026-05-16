#!/bin/bash

usage() {
cat << EOF
Usage: $(basename "$0") [OPTIONS]

Description:
  Print to screen queue partition messages.

Options:
  -h, --help              Show this help message and exit
  --config-map            *Configuration file path to load cluster node settings
  --queue				  *Queue to print messages from
  --partition		      *Partition from specified queue to print messages (0-indexed)
  --segment               *Index of segment to read messages from
  --offset                Message offset to read after (default 0)
  --messages-to-print     How many messages to print (default 10)

Examples:
  $(basename "$0") --config-map /test_path/settings.conf --queue temp --partition 0 --segment 3
  $(basename "$0") --config-map /test_path/settings.conf --queue temp --partition 0 --segment 1 --offset 10
  $(basename "$0") --config-map /test_path/settings.conf --queue temp --partition 0 --segment 1 --offset 10 --messages-to-print 100
EOF
exit 1
}