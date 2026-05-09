#!/bin/bash

to_int() {
	printf "%d" "$(( 16#$1 ))"
}

to_signed_int() {
    # 1. Convert hex to decimal (Bash handles this as 64-bit)
    local val=$(( 16#$1 ))
    
    # 2. Check if the 31st bit is set (greater than or equal to 2^31)
    # 0x80000000 is 2147483648
    if [ "$val" -ge 2147483648 ]; then
        # 3. Subtract 2^32 (4294967296) to get the negative signed value
        echo "$(( val - 4294967296 ))"
    else
        echo "$val"
    fi
}