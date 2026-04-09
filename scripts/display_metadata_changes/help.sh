usage() {
cat << EOF
Usage: $(basename "$0") [OPTIONS]

Description:
  Print to screen cluster metadata changes.

Options:
  -h, --help              Show this help message and exit
  --config-map            *Configuration file path to load cluster node settings
  --segment               *Index of segment to read cluster metadata changes from
  --offset                Message offset to read after (default 0)
  --messages-to-print     How many messages to print (default 10)

Examples:
  $(basename "$0") --config-map /test_path/settings.conf --segment 3
  $(basename "$0") --config-map /test_path/settings.conf --segment 1 --offset 10
  $(basename "$0") --config-map /test_path/settings.conf --segment 1 --offset 10 --messages-to-print 100
EOF
exit 1
}