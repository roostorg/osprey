#!/bin/bash

# Configuration variables
KAFKA_BROKER=${KAFKA_BROKER:-"localhost:9092"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"test-data"}
KAFKA_CONFIG_FILE=${KAFKA_CONFIG_FILE:-""}

# Function to generate random user ID
generate_random_user_id() {
    echo "user_$(shuf -i 100-9999 -n 1)"
}

# Function to generate current timestamp in ISO 8601 format with nanoseconds
generate_timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%S.%NZ"
}

# Initialize action_id counter
action_id=1

# words in post
words=(hello the quick brown fox jumps over lazy dog and cat runs fast);
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )


# Function to generate a single JSON action
generate_action() {
    local text="${words[RANDOM % ${#words[@]}]} ${words[RANDOM % ${#words[@]}]} ${words[RANDOM % ${#words[@]}]} ${words[RANDOM % ${#words[@]}]} ${words[RANDOM % ${#words[@]}]}."
    local timestamp=$(generate_timestamp)
    local user_id=$(generate_random_user_id)
    local ip_address="192.168.1.$(shuf -i 1-254 -n 1)"

    local sed_commands=()
    sed_commands+=("s/\$text/$text/g")
    sed_commands+=("s/\$timestamp/$timestamp/g")
    sed_commands+=("s/\$user_id/$user_id/g")
    sed_commands+=("s/\$ip_address/$ip_address/g")
    sed_commands+=("s/\$action_id/$action_id/g")

    # Apply all sed commands
    local cmd="sed"
    for sed_cmd in "${sed_commands[@]}"; do
        cmd="$cmd -e '$sed_cmd'"
    done
    eval "$cmd" "$SCRIPT_DIR/template.json"
}

# Function to build kafka-console-producer command
build_kafka_command() {
    local cmd="kafka-console-producer --broker-list $KAFKA_BROKER --topic $KAFKA_TOPIC"

    if [ -n "$KAFKA_CONFIG_FILE" ]; then
        cmd="$cmd --producer.config $KAFKA_CONFIG_FILE"
    fi

    echo "$cmd"
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
echo "Generating actions every second to Kafka topic '$KAFKA_TOPIC'..."
echo "Kafka broker: $KAFKA_BROKER"
if [ -n "$KAFKA_CONFIG_FILE" ]; then
    echo "Using config file: $KAFKA_CONFIG_FILE"
fi
echo "Press Ctrl+C to stop..."
echo

# Build the kafka command
kafka_cmd=$(build_kafka_command)

# Infinite loop to generate and send actions
while true; do
    action=$(generate_action)
    echo -e "Sending $action"
    echo -e "$action" | $kafka_cmd

    # Increment action_id in the main shell
    ((action_id++))
    sleep 1
done
