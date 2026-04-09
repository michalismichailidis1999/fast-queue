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

source ./common/file_read.sh

FILE_OFFSET=0

read_from_file "$METADATA_CHANGES_DIR_PATH/$SEGMENT_FILE" $FILE_OFFSET $READ_BATCH

total_messages_print=0

source ./common/byte_conversion.sh
source ./display_metadata_changes/metadata_parsing.sh

while (( $total_messages_print < $MESSAGES_TO_PRINT )); do
    if no_bytes_read; then
        break
    fi

    if ! parse_and_print_metadata_values; then
        FILE_OFFSET=$(( $FILE_OFFSET + $bytes_offset / 2 ))
    else
        FILE_OFFSET=$(( $FILE_OFFSET + $READ_BATCH ))
    fi
    
    if end_of_file_reached $READ_BATCH; then
        break
    else
        read_from_file "$METADATA_CHANGES_DIR_PATH/$SEGMENT_FILE" $FILE_OFFSET $READ_BATCH
    fi
done

# ./display_metadata_changes.sh --config-path D:/MessageBrokerHelperFiles/dummy.conf --segment 1