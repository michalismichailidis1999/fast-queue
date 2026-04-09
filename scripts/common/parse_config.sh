declare -A config # create associative array

while IFS= read -r line; do
  # Remove inline comments
  line="${line%%#*}"

  # Skip empty lines
  [[ -z "$line" ]] && continue

  # Split key and value
  key="${line%%=*}"
  value="${line#*=}"
  value="${value//$'\r'/}"

  # Store in hashmap
  config["$key"]="$value"

done < $CONFIG_PATH