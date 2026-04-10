#!/bin/bash

check_if_corrupted() {
	if (( total_bytes <= 0 )); then
		echo "Corrupted message detected"
		exit 1;
	fi
}