#!/bin/bash

source ./common/validations/is_number.sh

SEGMENT_FILE=""
SEGMENT_INDEX_FILE=""

set_segment_file() {
    SEGMENT_FILE="$(printf "%020d" "$1").txt"
    SEGMENT_INDEX_FILE="index_$SEGMENT_FILE"
}

source ./display_messages/help.sh
source ./display_messages/parse_config.sh

QUEUE_PARTITION_DIR_PATH="${config[log_path]}/$QUEUE/partition-$PARTITION"

source ./common/file_read.sh

FILE_OFFSET=$SEGMENT_INITIAL_OFFSET

read_from_file "$QUEUE_PARTITION_DIR_PATH/$SEGMENT_FILE" $FILE_OFFSET $READ_BATCH

total_messages_print=0

source ./common/byte_conversion.sh
source ./display_messages/messages_parsing.sh

more_in_current_batch=0

echo ""

while (( total_messages_print < MESSAGES_TO_PRINT )); do
    if no_bytes_read; then
        break
    fi

    if parse_and_print_messages; then
        FILE_OFFSET=$(( $FILE_OFFSET + $bytes_offset / 2 ))
        more_in_current_batch=1
    else
        FILE_OFFSET=$(( $FILE_OFFSET + $READ_BATCH ))
        more_in_current_batch=0
    fi

    if (( total_messages_print >= MESSAGES_TO_PRINT )); then
        break
    fi
    
    if (( more_in_current_batch == 1 )) || ! end_of_file_reached $READ_BATCH; then
        read_from_file "$QUEUE_PARTITION_DIR_PATH/$SEGMENT_FILE" $FILE_OFFSET $READ_BATCH
    else
        break
    fi
done