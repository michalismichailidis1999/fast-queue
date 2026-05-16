#!/bin/bash

RTG_COMMAND_NODE_ID_SIZE=8
RTG_COMMAND_NODE_ID_OFFSET=$COMMAND_TOTAL_BYTES
RTG_COMMAND_GROUP_ID_SIZE=16
RTG_COMMAND_GROUP_ID_OFFSET=$(( $RTG_COMMAND_NODE_ID_SIZE + $RTG_COMMAND_NODE_ID_OFFSET ))
RTG_COMMAND_QUEUES_COUNT_SIZE=8
RTG_COMMAND_QUEUES_COUNT_OFFSET=$(( $RTG_COMMAND_GROUP_ID_SIZE + $RTG_COMMAND_GROUP_ID_OFFSET ))

RTG_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
RTG_COMMAND_QUEUE_NAMES_OFFSET=$(( $RTG_COMMAND_QUEUES_COUNT_SIZE + $RTG_COMMAND_QUEUES_COUNT_OFFSET ))

UTG_COMMAND_NODE_ID_SIZE=8
UTG_COMMAND_NODE_ID_OFFSET=$COMMAND_TOTAL_BYTES
UTG_COMMAND_GROUP_ID_SIZE=16
UTG_COMMAND_GROUP_ID_OFFSET=$(( $UTG_COMMAND_NODE_ID_SIZE + $UTG_COMMAND_NODE_ID_OFFSET ))

tx_group_assigned_node=0
tx_group_id=0
tx_group_total_queues=0
tx_group_queue_size=0
tx_group_queue=""
tx_group_queue_offset=0

print_register_transaction_group_metadata_change_values() {
	byte_from=$(( $bytes_offset + $RTG_COMMAND_NODE_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RTG_COMMAND_NODE_ID_SIZE}")
	tx_group_assigned_node=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $RTG_COMMAND_GROUP_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RTG_COMMAND_GROUP_ID_SIZE}")
	tx_group_id=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $RTG_COMMAND_QUEUES_COUNT_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RTG_COMMAND_QUEUES_COUNT_SIZE}")
	tx_group_total_queues=$(to_int $current_hex)

	echo "Transaction Group Assigned Node: $tx_group_assigned_node"
	echo "Transaction Group Id: $tx_group_id"
	echo "Transaction Group Total Queues: $tx_group_total_queues"

	echo "Transaction Group Queues: "

	tx_group_queue_offset=$RTG_COMMAND_QUEUE_NAMES_OFFSET

	for ((i=0; i<$tx_group_total_queues; i++))
	do
		byte_from=$(( $tx_group_queue_offset + $tx_group_queue_offset ))
		current_hex=$(reverse_endian "${bytes_hex:byte_from:RTG_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
		tx_group_queue_size=$(to_int $current_hex)
		tx_group_queue_size=$(( $tx_group_queue_size * 2 ))

		byte_from=$(( $tx_group_queue_offset + $RTG_COMMAND_QUEUE_NAME_LENGTH_SIZE ))
		tx_group_queue=$(echo -n ${bytes_hex:byte_from:tx_group_queue_size} | xxd -r -p)

		tx_group_queue_offset=$(( $tx_group_queue_offset + $RTG_COMMAND_QUEUE_NAME_LENGTH_SIZE + $tx_group_queue_size ))

		echo "  - $tx_group_queue"
	done
}

print_unregister_transaction_group_metadata_change_values() {
	byte_from=$(( $bytes_offset + $UTG_COMMAND_NODE_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:UTG_COMMAND_NODE_ID_SIZE}")
	tx_group_assigned_node=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $UTG_COMMAND_GROUP_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:UTG_COMMAND_GROUP_ID_SIZE}")
	tx_group_id=$(to_int $current_hex)

	echo "Transaction Group Assigned Node: $tx_group_assigned_node"
	echo "Transaction Group Id: $tx_group_id"
}