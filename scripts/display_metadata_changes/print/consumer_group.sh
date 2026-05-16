#!/bin/bash

RCG_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
RCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET=$COMMAND_TOTAL_BYTES
RCG_COMMAND_QUEUE_NAME_SIZE=200
RCG_COMMAND_QUEUE_NAME_OFFSET=$(( $RCG_COMMAND_QUEUE_NAME_LENGTH_SIZE + $RCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
RCG_COMMAND_PARTITION_ID_SIZE=8
RCG_COMMAND_PARTITION_ID_OFFSET=$(( $RCG_COMMAND_QUEUE_NAME_SIZE + $RCG_COMMAND_QUEUE_NAME_OFFSET ))
RCG_COMMAND_GROUP_ID_LENGTH_SIZE=8
RCG_COMMAND_GROUP_ID_LENGTH_OFFSET=$(( $RCG_COMMAND_PARTITION_ID_SIZE + $RCG_COMMAND_PARTITION_ID_OFFSET ))
RCG_COMMAND_GROUP_ID_SIZE=150
RCG_COMMAND_GROUP_ID_OFFSET=$(( $RCG_COMMAND_GROUP_ID_LENGTH_SIZE + $RCG_COMMAND_GROUP_ID_LENGTH_OFFSET ))
RCG_COMMAND_CONSUMER_ID_SIZE=16
RCG_COMMAND_CONSUMER_ID_OFFSET=$(( $RCG_COMMAND_GROUP_ID_SIZE + $RCG_COMMAND_GROUP_ID_OFFSET ))
RCG_COMMAND_STOLE_FROM_CONSUMER_SIZE=16
RCG_COMMAND_STOLE_FROM_CONSUMER_OFFSET=$(( $RCG_COMMAND_CONSUMER_ID_SIZE + $RCG_COMMAND_CONSUMER_ID_OFFSET ))
RCG_COMMAND_CONSUME_FROM_BEGINNING_SIZE=2
RCG_COMMAND_CONSUME_FROM_BEGINNING_OFFSET=$(( $RCG_COMMAND_STOLE_FROM_CONSUMER_SIZE + $RCG_COMMAND_STOLE_FROM_CONSUMER_OFFSET ))

UCG_COMMAND_QUEUE_NAME_LENGTH_SIZE=8
UCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET=$COMMAND_TOTAL_BYTES
UCG_COMMAND_QUEUE_NAME_SIZE=200
UCG_COMMAND_QUEUE_NAME_OFFSET=$(( $UCG_COMMAND_QUEUE_NAME_LENGTH_SIZE + $UCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
UCG_COMMAND_PARTITION_ID_SIZE=8
UCG_COMMAND_PARTITION_ID_OFFSET=$(( $UCG_COMMAND_QUEUE_NAME_SIZE + $UCG_COMMAND_QUEUE_NAME_OFFSET ))
UCG_COMMAND_GROUP_ID_LENGTH_SIZE=8
UCG_COMMAND_GROUP_ID_LENGTH_OFFSET=$(( $UCG_COMMAND_PARTITION_ID_SIZE + $UCG_COMMAND_PARTITION_ID_OFFSET ))
UCG_COMMAND_GROUP_ID_SIZE=150
UCG_COMMAND_GROUP_ID_OFFSET=$(( $UCG_COMMAND_GROUP_ID_LENGTH_SIZE + $UCG_COMMAND_GROUP_ID_LENGTH_OFFSET ))
UCG_COMMAND_CONSUMER_ID_SIZE=16
UCG_COMMAND_CONSUMER_ID_OFFSET=$(( $UCG_COMMAND_GROUP_ID_SIZE + $UCG_COMMAND_GROUP_ID_OFFSET ))

consumer_queue_size=0
consumer_queue=""
consumer_partition=0
consumer_group_id_size=0
consumer_group_id=""
consumer_id=0
stole_from_consumer=0
consumer_from_beginning=0

print_consumer_group_register_metadata_change_values() {
	byte_from=$(( $bytes_offset + $RCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RCG_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
	consumer_queue_size=$(to_int $current_hex)
	consumer_queue_size=$(( $consumer_queue_size * 2 ))

	byte_from=$(( $bytes_offset + $RCG_COMMAND_QUEUE_NAME_OFFSET ))
	consumer_queue=$(echo -n ${bytes_hex:byte_from:consumer_queue_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $RCG_COMMAND_PARTITION_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RCG_COMMAND_PARTITION_ID_SIZE}")
	consumer_partition=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $RCG_COMMAND_GROUP_ID_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RCG_COMMAND_GROUP_ID_LENGTH_SIZE}")
	consumer_group_id_size=$(to_int $current_hex)
	consumer_group_id_size=$(( $consumer_group_id_size * 2 ))

	byte_from=$(( $bytes_offset + $RCG_COMMAND_GROUP_ID_OFFSET ))
	consumer_group_id=$(echo -n ${bytes_hex:byte_from:consumer_group_id_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $RCG_COMMAND_CONSUMER_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RCG_COMMAND_CONSUMER_ID_SIZE}")
	consumer_id=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $RCG_COMMAND_STOLE_FROM_CONSUMER_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RCG_COMMAND_STOLE_FROM_CONSUMER_SIZE}")
	stole_from_consumer=$(to_signed_int $current_hex)

	byte_from=$(( $bytes_offset + $RCG_COMMAND_CONSUME_FROM_BEGINNING_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RCG_COMMAND_CONSUME_FROM_BEGINNING_SIZE}")
	consumer_from_beginning=$(to_int $current_hex)

	echo "Consumer Queue: $consumer_queue"
	echo "Consumer Partition: $consumer_partition"
	echo "Consumer Group Id: $consumer_group_id"
	echo "Consumer Id: $consumer_id"
	echo "Stole From Consumer: $stole_from_consumer"
	echo "Consumer From Beginning: $consumer_from_beginning"
}

print_consumer_group_unregister_metadata_change_values() {
	byte_from=$(( $bytes_offset + $UCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:UCG_COMMAND_QUEUE_NAME_LENGTH_SIZE}")
	consumer_queue_size=$(to_int $current_hex)
	consumer_queue_size=$(( $consumer_queue_size * 2 ))

	byte_from=$(( $bytes_offset + $UCG_COMMAND_QUEUE_NAME_OFFSET ))
	consumer_queue=$(echo -n ${bytes_hex:byte_from:consumer_queue_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $UCG_COMMAND_PARTITION_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:UCG_COMMAND_PARTITION_ID_SIZE}")
	consumer_partition=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $UCG_COMMAND_GROUP_ID_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:UCG_COMMAND_GROUP_ID_LENGTH_SIZE}")
	consumer_group_id_size=$(to_int $current_hex)
	consumer_group_id_size=$(( $consumer_group_id_size * 2 ))

	byte_from=$(( $bytes_offset + $UCG_COMMAND_GROUP_ID_OFFSET ))
	consumer_group_id=$(echo -n ${bytes_hex:byte_from:consumer_group_id_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $UCG_COMMAND_CONSUMER_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:UCG_COMMAND_CONSUMER_ID_SIZE}")
	consumer_id=$(to_int $current_hex)

	echo "Consumer Queue: $consumer_queue"
	echo "Consumer Partition: $consumer_partition"
	echo "Consumer Group Id: $consumer_group_id"
	echo "Consumer Id: $consumer_id"
}