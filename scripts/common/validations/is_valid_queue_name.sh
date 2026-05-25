is_valid_queue_name() {
    if [ -z "$1" ]; then
        echo "Queue name cannot be empty" >&2
        exit 1
    fi

	if ! [[ "$1" =~ ^[a-zA-Z][a-zA-Z0-9_-]*$ ]]; then
        echo "Queue name $2 format is not valid" >&2
        exit 1
    fi
}