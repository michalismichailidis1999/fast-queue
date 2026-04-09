#!/bin/bash

reverse_endian() {
    local hex="$1"
    local len=${#hex}
    local rev=""

    for (( i=len-2; i>=0; i-=2 )); do
        rev+="${hex:i:2}"
    done

    echo "$rev"
}