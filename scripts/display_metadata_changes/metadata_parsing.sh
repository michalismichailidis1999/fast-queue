#!/bin/bash

get_metadata_type_description() {
	case "$1" in
	  1)
	    echo "CREATE_QUEUE"
		;;
	  2)
	    echo "DELETE_QUEUE"
		;;
	  3)
	    echo "ALTER_PARTITION_ASSIGNMENT"
		;;
	  4)
	    echo "ALTER_PARTITION_LEADER_ASSIGNMENT"
		;;
	  5)
	    echo "REGISTER_DATA_NODE"
		;;
	  6)
	    echo "UNREGISTER_DATA_NODE"
		;;
	  7)
	    echo "REGISTER_CONSUMER_GROUP"
		;;
	  8)
	    echo "UNREGISTER_CONSUMER_GROUP"
		;;
	  9)
	    echo "ADD_LAGGING_FOLLOWER"
		;;
	  10)
	    echo "REMOVE_LAGGING_FOLLOWER"
		;;
	  11)
	    echo "REGISTER_TRANSACTION_GROUP"
		;;
	  12)
	    echo "UNREGISTER_TRANSACTION_GROUP"
		;;
	  *)
        echo "Unknown metadata type: $1"
        #exit 1
        ;;
	esac
}

#Command Schema
TOTAL_METADATA_BYTES=8
TOTAL_METADATA_BYTES_OFFSET=0
VERSION_SIZE=8
VERSION_SIZE_OFFSET=$(( $TOTAL_METADATA_BYTES + $TOTAL_METADATA_BYTES_OFFSET ))
CHECKSUM_SIZE=16
CHECKSUM_OFFSET=$(( $VERSION_SIZE + $VERSION_SIZE_OFFSET ))
MESSAGE_ID_SIZE=16
MESSAGE_ID_OFFSET=$(( $CHECKSUM_SIZE + $CHECKSUM_OFFSET ))
MESSAGE_TIMESTAMP_SIZE=16
MESSAGE_TIMESTAMP_OFFSET=$(( $MESSAGE_ID_SIZE + $MESSAGE_ID_OFFSET ))
MESSAGE_IS_ACTIVE_SIZE=2
MESSAGE_IS_ACTIVE_OFFSET=$(( $MESSAGE_TIMESTAMP_SIZE + $MESSAGE_TIMESTAMP_OFFSET ))
MESSAGE_COMMIT_STATUS_SIZE=8
MESSAGE_COMMIT_STATUS_OFFSET=$(( $MESSAGE_IS_ACTIVE_SIZE + $MESSAGE_IS_ACTIVE_OFFSET ))
MESSAGE_LEADER_ID_SIZE=16 # Ignore this in metadata printing
MESSAGE_LEADER_ID_OFFSET=$(( $MESSAGE_COMMIT_STATUS_SIZE + $MESSAGE_COMMIT_STATUS_OFFSET )) # Ignore this in metadata printing
MESSAGE_KEY_SIZE=8
MESSAGE_KEY_OFFSET=$(( $MESSAGE_LEADER_ID_SIZE + $MESSAGE_LEADER_ID_OFFSET ))
MESSAGE_PAYLOAD_SIZE=8
MESSAGE_PAYLOAD_OFFSET=$(( $MESSAGE_KEY_SIZE + $MESSAGE_KEY_OFFSET ))
COMMAND_TYPE_SIZE=8
COMMAND_TYPE_OFFSET=$(( $MESSAGE_PAYLOAD_SIZE + $MESSAGE_PAYLOAD_OFFSET ))
COMMAND_TERM_SIZE=16
COMMAND_TERM_OFFSET=$(( $COMMAND_TYPE_SIZE + $COMMAND_TYPE_OFFSET ))

bytes_offset=0
byte_from=0
current_hex=""

total_bytes=0
command_type_id=0
new_offset=0
command_type=""

source ./display_metadata_changes/print/create_queue.sh

print_metadata_change() {
	echo =========== Metadata Change ====================

	echo "Total Bytes: $total_bytes"
	echo "Metadata Change Type: $command_type"

	case "$1" in
	  1)
	    print_create_queue_metadata_change_values
		;;
	  *)
        echo "Unknown metadata type: $1"
        #exit 1
        ;;
	esac

	echo ================================================
	echo ""
}

source ./common/reverse_endian.sh
source ./common/validations/is_corrupted.sh

parse_and_print_metadata_values() {
	bytes_offset=0

	while (( bytes_offset < actual_bytes_read && total_messages_print < MESSAGES_TO_PRINT )); do
		if (( actual_bytes_read - bytes_offset < TOTAL_METADATA_BYTES )); then
			return 0;
		fi

		byte_from=$(( $bytes_offset + $TOTAL_METADATA_BYTES_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:TOTAL_METADATA_BYTES}")
		total_bytes=$(to_int $current_hex)

		new_offset=$(( $bytes_offset + $total_bytes * 2 ))

		if (( new_offset > actual_bytes_read )); then
			return 0;
		fi

		byte_from=$(( $bytes_offset + $COMMAND_TYPE_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:COMMAND_TYPE_SIZE}")
		command_type_id=$(to_int $current_hex)

		command_type=$(get_metadata_type_description $command_type_id)

		# TODO: Convert all common command schema values here

		check_if_corrupted

		# TODO: Check if message id > OFFSET passed in script options

		print_metadata_change

		total_messages_print=$(( $total_messages_print + 1 ))
		bytes_offset=$(( $bytes_offset + $total_bytes * 2 ))
	done

	return 1;
}