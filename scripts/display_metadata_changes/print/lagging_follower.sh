#!/bin/bash

ALF_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
ALF_COMMAND_QUEUE_NAME_LENGTH_OFFSET=$COMMAND_TOTAL_BYTES
ALF_COMMAND_QUEUE_NAME_SIZE=200
ALF_COMMAND_QUEUE_NAME_OFFSET=$(( $ALF_COMMAND_QUEUE_NAME_LENGTH_SIZE + $ALF_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
ALF_COMMAND_PARTITION_ID_SIZE=8
ALF_COMMAND_PARTITION_ID_OFFSET=$(( $ALF_COMMAND_QUEUE_NAME_SIZE + $ALF_COMMAND_QUEUE_NAME_OFFSET ))
ALF_COMMAND_NODE_ID_SIZE=8
ALF_COMMAND_NODE_ID_OFFSET=$(( $ALF_COMMAND_PARTITION_ID_SIZE + $ALF_COMMAND_PARTITION_ID_OFFSET ))

RLF_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
RLF_COMMAND_QUEUE_NAME_LENGTH_OFFSET=$COMMAND_TOTAL_BYTES
RLF_COMMAND_QUEUE_NAME_SIZE=200
RLF_COMMAND_QUEUE_NAME_OFFSET=$(( $RLF_COMMAND_QUEUE_NAME_LENGTH_SIZE + $RLF_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
RLF_COMMAND_PARTITION_ID_SIZE=8
RLF_COMMAND_PARTITION_ID_OFFSET=$(( $RLF_COMMAND_QUEUE_NAME_SIZE + $RLF_COMMAND_QUEUE_NAME_OFFSET ))
RLF_COMMAND_NODE_ID_SIZE=8
RLF_COMMAND_NODE_ID_OFFSET=$(( $RLF_COMMAND_PARTITION_ID_SIZE + $RLF_COMMAND_PARTITION_ID_OFFSET ))

lagging_follower_queue_size=0
lagging_follower_queue=""
lagging_follower_partition=0
lagging_follower_node=0

print_add_lagging_follower_metadata_change_values() {
	byte_from=$(( $bytes_offset + $ALF_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:ALF_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
	lagging_follower_queue_size=$(to_int $current_hex)
	lagging_follower_queue_size=$(( $lagging_follower_queue_size * 2 ))

	byte_from=$(( $bytes_offset + $ALF_COMMAND_QUEUE_NAME_OFFSET ))
	lagging_follower_queue=$(echo -n ${bytes_hex:byte_from:lagging_follower_queue_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $ALF_COMMAND_PARTITION_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:ALF_COMMAND_PARTITION_ID_SIZE}")
	lagging_follower_partition=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $ALF_COMMAND_NODE_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:ALF_COMMAND_NODE_ID_SIZE}")
	lagging_follower_node=$(to_int $current_hex)

	echo "Lagging Follower Queue: $lagging_follower_queue"
	echo "Lagging Follower Partition: $lagging_follower_partition"
	echo "Lagging Follower Node Id: $lagging_follower_node"
}

print_remove_lagging_follower_metadata_change_values() {
	byte_from=$(( $bytes_offset + $RLF_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RLF_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
	lagging_follower_queue_size=$(to_int $current_hex)
	lagging_follower_queue_size=$(( $lagging_follower_queue_size * 2 ))

	byte_from=$(( $bytes_offset + $RLF_COMMAND_QUEUE_NAME_OFFSET ))
	lagging_follower_queue=$(echo -n ${bytes_hex:byte_from:lagging_follower_queue_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $RLF_COMMAND_PARTITION_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RLF_COMMAND_PARTITION_ID_SIZE}")
	lagging_follower_partition=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $RLF_COMMAND_NODE_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RLF_COMMAND_NODE_ID_SIZE}")
	lagging_follower_node=$(to_int $current_hex)

	echo "Lagging Follower Queue: $lagging_follower_queue"
	echo "Lagging Follower Partition: $lagging_follower_partition"
	echo "Lagging Follower Node Id: $lagging_follower_node"
}