#!/bin/bash

source ./common/validations/is_number.sh

SEGMENT_FILE=""
SEGMENT_INDEX_FILE=""

set_segment_file() {
    SEGMENT_FILE="$(printf "%020d" "$1").txt"
    SEGMENT_INDEX_FILE="index_$SEGMENT_FILE"
}

source ./display_metadata_changes/help.sh
source ./display_metadata_changes/parse_config.sh

METADATA_CHANGES_DIR_PATH="${config[log_path]}/__cluster_metadata"

ls $METADATA_CHANGES_DIR_PATH
echo "Printing messages ($MESSAGES_TO_PRINT messages) from segment $SEGMENT_FILE with index file $SEGMENT_INDEX_FILE starting after offset $OFFSET"

# ./display_metadata_changes.sh --config-path D:/MessageBrokerHelperFiles/dummy.conf