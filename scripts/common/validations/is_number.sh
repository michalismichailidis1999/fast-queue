check_if_integer() {
	if ! [[ "$1" =~ ^-?[0-9]+$ ]]; then
        echo "$2 needs to be an integer (value passed was $1)" >&2
        exit 1
    fi
}

check_if_positive_integer() {
	if ! [[ "$1" =~ ^[0-9]+$ ]]; then
        echo "$2 needs to be a positive integer (value passed was $1)" >&2
        exit 1
    fi
}