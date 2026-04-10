#!/bin/bash

BLOCK_SIZE=4096 # 4KB
READ_BATCH=$(( $BLOCK_SIZE * 4 )) # 16KB
SEGMENT_INITIAL_OFFSET=42

bytes_hex=""
bytes_read=0
actual_bytes_read=0

# Arg 1: File
# Arg 2: Offset
# Arg 3: Bytes to read
read_from_file() {
	bytes_hex=$(dd if=$1 bs=1 skip=$2 count=$3 status=none | xxd -p -c 0)

	# Length in bytes
    bytes_read=$(( ${#bytes_hex} / 2 ))   # each byte = 2 hex chars

	actual_bytes_read=${#bytes_hex}
}

# Use after each usage of read_from_file to check if end of file reached
# Arg 1: Bytes tries to read in read_from_file usage before
end_of_file_reached() {
	if (( $bytes_read <= $1 )); then
		return 0;
	fi

	return 1;
}

no_bytes_read() {
	if (( $bytes_read == 0 )); then
		return 0;
	fi

	return 1;
}