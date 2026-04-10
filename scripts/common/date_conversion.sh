#!/bin/bash

seconds=0
datetime=""

convert_milli_to_datetime() {
	seconds=$(( $1 / 1000 ))

	# Convert to a specific format (e.g., YYYY-MM-DD HH:MM:SS)
	datetime=$(date -d "@$seconds" +"%Y-%m-%d %H:%M:%S")

	echo "$datetime"
}