#!/bin/bash

PA_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
PA_COMMAND_QUEUE_NAME_LENGTH_OFFSET=$COMMAND_TOTAL_BYTES
PA_COMMAND_QUEUE_NAME_SIZE=200
PA_COMMAND_QUEUE_NAME_OFFSET=$(( $PA_COMMAND_QUEUE_NAME_LENGTH_SIZE + $PA_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
PA_COMMAND_PARTITION_SIZE=8
PA_COMMAND_PARTITION_OFFSET=$(( $PA_COMMAND_QUEUE_NAME_SIZE + $PA_COMMAND_QUEUE_NAME_OFFSET ))
PA_COMMAND_TO_NODE_SIZE=8
PA_COMMAND_TO_NODE_OFFSET=$(( $PA_COMMAND_PARTITION_SIZE + $PA_COMMAND_PARTITION_OFFSET ))
PA_COMMAND_FROM_NODE_SIZE=8
PA_COMMAND_FROM_NODE_OFFSET=$(( $PA_COMMAND_TO_NODE_SIZE + $PA_COMMAND_TO_NODE_OFFSET ))

PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET=$COMMAND_TOTAL_BYTES
PLA_COMMAND_QUEUE_NAME_SIZE=200
PLA_COMMAND_QUEUE_NAME_OFFSET=$(( $PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE + $PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
PLA_COMMAND_PARTITION_SIZE=8
PLA_COMMAND_PARTITION_OFFSET=$(( $PLA_COMMAND_QUEUE_NAME_SIZE + $PLA_COMMAND_QUEUE_NAME_OFFSET ))
PLA_COMMAND_LEADER_EPOCH_SIZE=16
PLA_COMMAND_LEADER_EPOCH_OFFSET=$(( $PLA_COMMAND_PARTITION_SIZE + $PLA_COMMAND_PARTITION_OFFSET ))
PLA_COMMAND_NEW_LEADER_NODE_SIZE=8
PLA_COMMAND_NEW_LEADER_NODE_OFFSET=$(( $PLA_COMMAND_LEADER_EPOCH_SIZE + $PLA_COMMAND_LEADER_EPOCH_OFFSET ))
PLA_COMMAND_PREV_LEADER_NODE_SIZE=8
PLA_COMMAND_PREV_LEADER_NODE_OFFSET=$(( $PLA_COMMAND_NEW_LEADER_NODE_SIZE + $PLA_COMMAND_NEW_LEADER_NODE_OFFSET ))

assigned_queue_name_size=0
assigned_queue_name=""
partition=0
from_node=0
to_node=0
leader_epoch=0

print_partition_assignment_metadata_change_values() {
	byte_from=$(( $bytes_offset + $PA_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PA_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
	assigned_queue_name_size=$(to_int $current_hex)
	assigned_queue_name_size=$(( $assigned_queue_name_size * 2 ))

	byte_from=$(( $bytes_offset + $PA_COMMAND_QUEUE_NAME_OFFSET ))
	assigned_queue_name=$(echo -n ${bytes_hex:byte_from:assigned_queue_name_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $PA_COMMAND_PARTITION_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PA_COMMAND_PARTITION_SIZE}")
	partition=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $PA_COMMAND_FROM_NODE_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PA_COMMAND_FROM_NODE_SIZE}")
	from_node=$(to_signed_int $current_hex)

	byte_from=$(( $bytes_offset + $PA_COMMAND_TO_NODE_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PA_COMMAND_TO_NODE_SIZE}")
	to_node=$(to_int $current_hex)

	echo "Assigned Queue: $assigned_queue_name"
	echo "Partition: $partition"
	echo "From Node: $from_node"
	echo "To Node: $to_node"
}

print_partition_leader_assignment_metadata_change_values() {
	byte_from=$(( $bytes_offset + $PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
	assigned_queue_name_size=$(to_int $current_hex)
	assigned_queue_name_size=$(( $assigned_queue_name_size * 2 ))

	byte_from=$(( $bytes_offset + $PLA_COMMAND_QUEUE_NAME_OFFSET ))
	assigned_queue_name=$(echo -n ${bytes_hex:byte_from:assigned_queue_name_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $PLA_COMMAND_PARTITION_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PLA_COMMAND_PARTITION_SIZE}")
	partition=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $PLA_COMMAND_LEADER_EPOCH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PLA_COMMAND_LEADER_EPOCH_SIZE}")
	leader_epoch=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $PLA_COMMAND_PREV_LEADER_NODE_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PLA_COMMAND_PREV_LEADER_NODE_SIZE}")
	from_node=$(to_signed_int $current_hex)

	byte_from=$(( $bytes_offset + $PLA_COMMAND_NEW_LEADER_NODE_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:PLA_COMMAND_NEW_LEADER_NODE_SIZE}")
	to_node=$(to_int $current_hex)

	echo "Assigned Queue: $assigned_queue_name"
	echo "Partition: $partition"
	echo "Leader Epoch: $leader_epoch"
	echo "From Node: $from_node"
	echo "To Node: $to_node"
}