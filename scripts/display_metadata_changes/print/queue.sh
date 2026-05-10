#!/bin/bash

CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
CQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET=$COMMAND_TOTAL_BYTES
CQ_COMMAND_QUEUE_NAME_SIZE=200
CQ_COMMAND_QUEUE_NAME_OFFSET=$(( $CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE + $CQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
CQ_COMMAND_PARTITION_SIZE=8
CQ_COMMAND_PARTITION_OFFSET=$(( $CQ_COMMAND_QUEUE_NAME_SIZE + $CQ_COMMAND_QUEUE_NAME_OFFSET ))
CQ_COMMAND_REPLICATION_SIZE=8
CQ_COMMAND_REPLICATION_OFFSET=$(( $CQ_COMMAND_PARTITION_SIZE + $CQ_COMMAND_PARTITION_OFFSET ))

DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
DQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET=$COMMAND_TOTAL_BYTES
DQ_COMMAND_QUEUE_NAME_SIZE=200
DQ_COMMAND_QUEUE_NAME_OFFSET=$(( $DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE + $DQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))

queue_name_size=0
queue_name=""
partitions=0
replication_factor=0

print_create_queue_metadata_change_values() {
	byte_from=$(( $bytes_offset + $CQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
	queue_name_size=$(to_int $current_hex)
	queue_name_size=$(( $queue_name_size * 2 ))

	byte_from=$(( $bytes_offset + $CQ_COMMAND_QUEUE_NAME_OFFSET ))
	queue_name=$(echo -n ${bytes_hex:byte_from:queue_name_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $CQ_COMMAND_PARTITION_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:CQ_COMMAND_PARTITION_SIZE}")
	partitions=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $CQ_COMMAND_REPLICATION_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:CQ_COMMAND_REPLICATION_SIZE}")
	replication_factor=$(to_int $current_hex)

	echo "Created Queue: $queue_name"
	echo "Partitions: $partitions"
	echo "Repliction Factor: $replication_factor"
}

print_delete_queue_metadata_change_values() {
	byte_from=$(( $bytes_offset + $DQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
	queue_name_size=$(to_int $current_hex)
	queue_name_size=$(( $queue_name_size * 2 ))

	byte_from=$(( $bytes_offset + $DQ_COMMAND_QUEUE_NAME_OFFSET ))
	queue_name=$(echo -n ${bytes_hex:byte_from:queue_name_size} | xxd -r -p)

	echo "Deleted Queue: $queue_name"
}