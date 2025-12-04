#!/bin/bash

# continuously generate and send test actions to the osprey coordinator via gRPC
# this mimics the Kafka test data generator but sends directly to the coordinator

set -e

COORDINATOR_HOST="${COORDINATOR_HOST:-localhost:19951}"

# Check if grpcurl is installed
if ! command -v grpcurl &> /dev/null; then
    echo "Error: grpcurl is not installed."
    echo "Install it with: brew install grpcurl (macOS) or go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed."
    echo "Install it with: brew install jq (macOS)"
    exit 1
fi

# Initialize action_id counter
action_id=1

# Words to randomly generate post content
words=(hello the quick brown fox jumps over lazy dog and cat runs fast)

# Get script directory
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Function to generate random user ID
generate_random_user_id() {
    echo "user_$(shuf -i 100-9999 -n 1)"
}

# Function to generate current timestamp in RFC3339 format
generate_timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%S.000000000Z"
}

# Function to generate random post text
generate_random_text() {
    echo "${words[RANDOM % ${#words[@]}]} ${words[RANDOM % ${#words[@]}]} ${words[RANDOM % ${#words[@]}]} ${words[RANDOM % ${#words[@]}]} ${words[RANDOM % ${#words[@]}]}."
}

# Function to generate action data from template
generate_action() {
    local text=$(generate_random_text)
    local timestamp=$(generate_timestamp)
    local user_id=$(generate_random_user_id)
    local ip_address="192.168.1.$(shuf -i 1-254 -n 1)"

    local sed_commands=()
    sed_commands+=("s/\$text/$text/g")
    sed_commands+=("s/\$timestamp/$timestamp/g")
    sed_commands+=("s/\$user_id/$user_id/g")
    sed_commands+=("s/\$ip_address/$ip_address/g")
    sed_commands+=("s/\$action_id/$action_id/g")

    # Apply all sed commands to template.json
    local cmd="sed"
    for sed_cmd in "${sed_commands[@]}"; do
        cmd="$cmd -e '$sed_cmd'"
    done
    eval "$cmd" "$SCRIPT_DIR/template.json"
}

# Function to send a single action
send_action() {
    local kafka_format_json=$(generate_action)
    
    # Extract the data object from the Kafka format and convert to coordinator format
    local action_data=$(echo "$kafka_format_json" | jq -c '.data')
    local timestamp=$(echo "$kafka_format_json" | jq -r '.send_time')
    local action_name=$(echo "$action_data" | jq -r '.action_name')
    local data_payload=$(echo "$action_data" | jq -c '.data')
    
    echo "[$action_id] Sending action - Name: $action_name, Timestamp: $timestamp"
    
    # Build gRPC request format
    jq -n \
      --arg action_id "$action_id" \
      --arg action_name "$action_name" \
      --argjson data_payload "$data_payload" \
      --arg timestamp "$timestamp" \
      '{
        action_id: ($action_id | tonumber),
        action_name: $action_name,
        action_data_json: ($data_payload | tostring),
        timestamp: $timestamp
      }' | grpcurl -plaintext -d @ "$COORDINATOR_HOST" \
        osprey.rpc.osprey_coordinator.sync_action.v1.OspreyCoordinatorSyncActionService/ProcessAction

    # Increment action_id
    ((action_id++))
}

# Function to handle cleanup on script termination
cleanup() {
    echo
    echo "Stopping data generation..."
    exit 0
}

# Set up signal handlers for graceful shutdown
trap cleanup SIGINT SIGTERM

# Main execution
echo "Generating actions every second to Osprey Coordinator at $COORDINATOR_HOST"
echo "Press Ctrl+C to stop..."
echo

# Infinite loop to generate and send actions
while true; do
    send_action
    sleep 1
done
