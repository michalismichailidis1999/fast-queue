#!/bin/bash

get_commit_status_description() {
	case "$1" in
	  0)
	    echo "UNCOMMITED"
		;;
	  1)
	    echo "COMMITED"
		;;
	  2)
	    echo "ABORTED"
		;;
	  *)
        echo "Unknown commit status: $1"
        exit 1
        ;;
	esac
}

#Message Schema
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
MESSAGE_LEADER_ID_SIZE=16
MESSAGE_LEADER_ID_OFFSET=$(( $MESSAGE_COMMIT_STATUS_SIZE + $MESSAGE_COMMIT_STATUS_OFFSET ))
MESSAGE_KEY_SIZE=8
MESSAGE_KEY_OFFSET=$(( $MESSAGE_LEADER_ID_SIZE + $MESSAGE_LEADER_ID_OFFSET ))
MESSAGE_PAYLOAD_SIZE=8
MESSAGE_PAYLOAD_OFFSET=$(( $MESSAGE_KEY_SIZE + $MESSAGE_KEY_OFFSET ))

COMMON_METADATA_TOTAL_BYTES=$(( $TOTAL_METADATA_BYTES + $VERSION_SIZE + $CHECKSUM_SIZE ))
MESSAGE_TOTAL_BYTES=$(( $COMMON_METADATA_TOTAL_BYTES + $MESSAGE_ID_SIZE + $MESSAGE_TIMESTAMP_SIZE + $MESSAGE_IS_ACTIVE_SIZE + $MESSAGE_COMMIT_STATUS_SIZE + $MESSAGE_LEADER_ID_SIZE + $MESSAGE_KEY_SIZE + $MESSAGE_PAYLOAD_SIZE ))

bytes_offset=0
byte_from=0
current_hex=""
new_offset=0

total_bytes=0
checksum=0
version=""
message_id=0
message_timestamp_milli=0
message_timestamp=""
is_active=0
commit_status=0
commit_status_desc=""
message_leader_id=0
key_size=0
payload_size=0
key=""
payload=""

computed_checksum_total_bytes=0
computed_checksum=0

source ./common/reverse_endian.sh
source ./common/validations/is_corrupted.sh
source ./common/version_conversion.sh
source ./common/date_conversion.sh

print_message() {
	echo "=========== Message $message_id ===================="

	echo "Total Bytes: $total_bytes"
	echo "Version: $version"
	echo "Checksum: $checksum"
	echo "Message Offset: $message_id"
	echo "Message Timestamp: $message_timestamp (UTC)"
	echo "Is Active: $is_active"
	echo "Commit Status: $commit_status_desc"
	echo "Message Leader Id: $message_leader_id"
	echo "Key: $key"
	echo "Payload: $payload"

	echo ================================================
	echo ""
}

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

		byte_from=$(( $bytes_offset + $CHECKSUM_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:CHECKSUM_SIZE}")
		checksum=$(to_int $current_hex)

		byte_from=$(( $bytes_offset + $COMMON_METADATA_TOTAL_BYTES ))
		computed_checksum_total_bytes=$(( $total_bytes * 2 - $COMMON_METADATA_TOTAL_BYTES ))
		computed_checksum=$(echo -n "${bytes_hex:byte_from:computed_checksum_total_bytes}" | xxd -r -p | perl -e '
			$crc = 0xFFFFFFFF;
			while(read(STDIN, $buffer, 1)) {
				$crc ^= ord($buffer);
				for (0..7) {
					$crc = ($crc >> 1) ^ ($crc & 1 ? 0xEDB88320 : 0);
				}
			}
			printf("%08x\n", $crc ^ 0xFFFFFFFF);
		')
		computed_checksum=$((16#$computed_checksum))

		check_if_corrupted

		byte_from=$(( $bytes_offset + $MESSAGE_ID_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:MESSAGE_ID_SIZE}")
		message_id=$(to_int $current_hex)

		if (( $message_id <= $OFFSET )); then
			bytes_offset=$(( $bytes_offset + $total_bytes * 2 ))
			continue
		fi

		byte_from=$(( $bytes_offset + $VERSION_SIZE_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:VERSION_SIZE}")
		version=$(convert_version_to_str $current_hex)

		byte_from=$(( $bytes_offset + $MESSAGE_TIMESTAMP_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:MESSAGE_TIMESTAMP_SIZE}")
		message_timestamp_milli=$(to_int $current_hex)

		message_timestamp=$( convert_milli_to_datetime $message_timestamp_milli )

		byte_from=$(( $bytes_offset + $MESSAGE_IS_ACTIVE_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:MESSAGE_IS_ACTIVE_SIZE}")
		is_active=$(to_int $current_hex)

		byte_from=$(( $bytes_offset + $MESSAGE_COMMIT_STATUS_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:MESSAGE_COMMIT_STATUS_SIZE}")
		commit_status=$(to_int $current_hex)

		commit_status_desc=$(get_commit_status_description $commit_status)

		byte_from=$(( $bytes_offset + $MESSAGE_LEADER_ID_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:MESSAGE_LEADER_ID_SIZE}")
		message_leader_id=$(to_int $current_hex)
		
		byte_from=$(( $bytes_offset + $MESSAGE_KEY_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:MESSAGE_KEY_SIZE}")
		key_size=$(to_int $current_hex)

		byte_from=$(( $bytes_offset + $MESSAGE_PAYLOAD_OFFSET ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:MESSAGE_PAYLOAD_SIZE}")
		payload_size=$(to_int $current_hex)

		if (( $key_size <= 0 )); then
			key="NULL"
		else
			byte_from=$(( $bytes_offset + $MESSAGE_TOTAL_BYTES ))
			key=$(echo -n ${bytes_hex:byte_from:key_size} | xxd -r -p)
		fi

		if (( $payload_size <= 0 )); then
			payload="NULL"
		else
			byte_from=$(( $bytes_offset + $MESSAGE_TOTAL_BYTES + $key_size ))
			payload=$(echo -n ${bytes_hex:byte_from:payload_size} | xxd -r -p)
		fi

		print_message

		total_messages_print=$(( $total_messages_print + 1 ))
		bytes_offset=$new_offset
	done

	return 1;
}