#!/bin/bash

RDN_COMMAND_NODE_ID_SIZE=8
RDN_COMMAND_NODE_ID_OFFSET=$COMMAND_TOTAL_BYTES
RDN_COMMAND_ADDRESS_LENGTH_SIZE=8
RDN_COMMAND_ADDRESS_LENGTH_OFFSET=$(( $RDN_COMMAND_NODE_ID_SIZE + $RDN_COMMAND_NODE_ID_OFFSET ))
RDN_COMMAND_ADDRESS_SIZE=78
RDN_COMMAND_ADDRESS_OFFSET=$(( $RDN_COMMAND_ADDRESS_LENGTH_SIZE + $RDN_COMMAND_ADDRESS_LENGTH_OFFSET ))
RDN_COMMAND_PORT_SIZE=8
RDN_COMMAND_PORT_OFFSET=$(( $RDN_COMMAND_ADDRESS_SIZE + $RDN_COMMAND_ADDRESS_OFFSET ))
RDN_COMMAND_EXT_ADDRESS_LENGTH_SIZE=8
RDN_COMMAND_EXT_ADDRESS_LENGTH_OFFSET=$(( $RDN_COMMAND_PORT_SIZE + $RDN_COMMAND_PORT_OFFSET ))
RDN_COMMAND_EXT_ADDRESS_SIZE=78
RDN_COMMAND_EXT_ADDRESS_OFFSET=$(( $RDN_COMMAND_EXT_ADDRESS_LENGTH_SIZE + $RDN_COMMAND_EXT_ADDRESS_LENGTH_OFFSET ))
RDN_COMMAND_EXT_PORT_SIZE=8
RDN_COMMAND_EXT_PORT_OFFSET=$(( $RDN_COMMAND_EXT_ADDRESS_SIZE + $RDN_COMMAND_EXT_ADDRESS_OFFSET ))

UDN_COMMAND_NODE_ID_SIZE=8
UDN_COMMAND_NODE_ID_OFFSET=$COMMAND_TOTAL_BYTES

registered_node_id=0
address_size=0
address=""
port=0
external_address_size=0
external_address=""
external_port=0

print_register_data_node_metadata_change_values() {
	byte_from=$(( $bytes_offset + $RDN_COMMAND_NODE_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RDN_COMMAND_NODE_ID_SIZE}")
	registered_node_id=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $RDN_COMMAND_ADDRESS_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RDN_COMMAND_ADDRESS_LENGTH_SIZE}")
	address_size=$(to_int $current_hex)
	address_size=$(( $address_size * 2 ))

	byte_from=$(( $bytes_offset + $RDN_COMMAND_ADDRESS_OFFSET ))
	address=$(echo -n ${bytes_hex:byte_from:address_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $RDN_COMMAND_PORT_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RDN_COMMAND_PORT_SIZE}")
	port=$(to_int $current_hex)

	byte_from=$(( $bytes_offset + $RDN_COMMAND_EXT_ADDRESS_LENGTH_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RDN_COMMAND_EXT_ADDRESS_LENGTH_SIZE}")
	external_address_size=$(to_int $current_hex)
	external_address_size=$(( $external_address_size * 2 ))

	byte_from=$(( $bytes_offset + $RDN_COMMAND_EXT_ADDRESS_OFFSET ))
	external_address=$(echo -n ${bytes_hex:byte_from:external_address_size} | xxd -r -p)

	byte_from=$(( $bytes_offset + $RDN_COMMAND_EXT_PORT_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:RDN_COMMAND_EXT_PORT_SIZE}")
	external_port=$(to_int $current_hex)

	echo "Registered Node: $registered_node_id"
	echo "Node Internal Address: $address:$port"
	echo "Node External Address: $external_address:$external_port"
}

print_unregister_data_node_metadata_change_values() {
	byte_from=$(( $bytes_offset + $UDN_COMMAND_NODE_ID_OFFSET ))
	current_hex=$(reverse_endian "${bytes_hex:byte_from:UDN_COMMAND_NODE_ID_SIZE}")
	registered_node_id=$(to_int $current_hex)

	echo "Unregistered Node: $registered_node_id"
}