#!/bin/bash

to_int() {
	printf "%d" "$(( 16#$1 ))"
}