#!/bin/bash

check_if_corrupted() {
	actual_file_offset=$(( $file_read_offset + $bytes_offset / 2 ))
	if (( total_bytes <= 0 )); then
		echo "Corrupted message detected at message starting at file offset $actual_file_offset (Message bytes <= 0)"
		exit 1;
	fi

	if (( checksum != computed_checksum )); then
		echo "Corrupted message detected at message starting at file offset $actual_file_offset (Invalid checksum)"
		exit 1;
	fi
}