#!/bin/bash

# Demo Data Generator for Osprey Conference Demo
# Generates realistic test data including:
# - Identity changes (handle/display name changes)
# - Direct messages (with spam patterns)
# - Posts (with embeds, mentions, and URLs)
# - Metadata (IP addresses, user agents)

# Configuration variables
# For Docker: use osprey-kafka:29092 as broker (internal Docker network)
# For external: use localhost:9092
KAFKA_BROKER=${KAFKA_BROKER:-"osprey-kafka:29092"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"osprey.actions_input"}
KAFKA_CONFIG_FILE=${KAFKA_CONFIG_FILE:-""}
USE_DOCKER=${USE_DOCKER:-"true"}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TEMPLATES_DIR="$SCRIPT_DIR/templates"

# Function to generate unique action_id using timestamp + random
generate_action_id() {
    # Use timestamp (seconds since epoch) + random suffix for uniqueness
    local timestamp=$(date +%s)
    local random_suffix=$(random_in_range 1000 9999)
    echo "${timestamp}${random_suffix}"
}

# Word lists for generating realistic content
normal_words=(the quick brown fox jumps over lazy dog and cat runs fast today is great)
spam_words=("FREE MONEY" "click-here.com" "bit.ly/scam" "limited-offer" "guaranteed returns" "crypto investment")
handles=(alice bob charlie diana emma frank grace henry ivy jack kate leo mia noah olivia)
spam_handles=(admin_official support_team moderator_real staff_member official_account)
display_names=("Alice Smith" "Bob Jones" "Charlie Brown" "Diana Prince" "Real User")
spam_display_names=("FREE GIVEAWAY" "DM ME NOW" "CLICK HERE" "WIN BIG")
user_agents=(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15"
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36"
)
suspicious_user_agents=(
    "python-requests/2.28.0"
    "curl/7.79.1"
    "Headless Chrome"
    "bot-scraper/1.0"
    "automated-tool"
)

# Function to generate random number in range (macOS compatible)
random_in_range() {
    local min=$1
    local max=$2
    echo $((min + RANDOM % (max - min + 1)))
}

# Function to generate random user ID
generate_random_user_id() {
    echo "user_$(random_in_range 100 9999)"
}

# Function to generate current timestamp in ISO 8601 format
generate_timestamp() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS: use gdate if available, otherwise use date without nanoseconds
        if command -v gdate &> /dev/null; then
            gdate -u +"%Y-%m-%dT%H:%M:%S.%NZ"
        else
            date -u +"%Y-%m-%dT%H:%M:%S.000000000Z"
        fi
    else
        date -u +"%Y-%m-%dT%H:%M:%S.%NZ"
    fi
}

# Function to generate a past timestamp for account creation
generate_account_created_at() {
    local days_ago=$((RANDOM % 365))
    if [[ "$OSTYPE" == "darwin"* ]]; then
        date -u -v-${days_ago}d +"%Y-%m-%dT%H:%M:%SZ"
    else
        date -u -d "$days_ago days ago" +"%Y-%m-%dT%H:%M:%SZ"
    fi
}

# Function to generate IP address
generate_ip_address() {
    local type=$1
    case $type in
        normal)
            echo "192.168.1.$(random_in_range 1 254)"
            ;;
        suspicious)
            echo "10.0.$(random_in_range 1 254).$(random_in_range 1 254)"
            ;;
        *)
            echo "192.168.1.$(random_in_range 1 254)"
            ;;
    esac
}

# Function to pick random element from array
pick_random() {
    local arr=("$@")
    echo "${arr[RANDOM % ${#arr[@]}]}"
}

# Function to escape special characters for JSON
escape_json() {
    echo "$1" | sed 's/"/\\"/g' | sed "s/'/\\'/g"
}

# Function to generate a create_post event
generate_create_post() {
    local is_spam=$1
    local text=""
    local embed_urls=""
    local mentions=""
    local extracted_urls=""
    local user_agent=""
    local ip_address=""
    local post_count=$((RANDOM % 200))
    local follower_count=$((RANDOM % 1000))

    if [ "$is_spam" = "true" ]; then
        text="$(pick_random "${spam_words[@]}") Check this out: $(pick_random "${spam_words[@]}")"
        embed_urls='"https://spam-site.com/offer"'
        extracted_urls='"https://bit.ly/scam123"'
        user_agent=$(pick_random "${suspicious_user_agents[@]}")
        ip_address=$(generate_ip_address suspicious)
        post_count=$((RANDOM % 10))
        follower_count=$((RANDOM % 3))
    else
        text="${normal_words[RANDOM % ${#normal_words[@]}]} ${normal_words[RANDOM % ${#normal_words[@]}]} ${normal_words[RANDOM % ${#normal_words[@]}]} ${normal_words[RANDOM % ${#normal_words[@]}]}."
        embed_urls=""
        mentions="\"@$(pick_random "${handles[@]}")\""
        extracted_urls=""
        user_agent=$(pick_random "${user_agents[@]}")
        ip_address=$(generate_ip_address normal)
    fi

    local timestamp=$(generate_timestamp)
    local user_id=$(generate_random_user_id)
    local account_created_at=$(generate_account_created_at)
    local action_id=$(generate_action_id)

    # Escape text for JSON
    text=$(escape_json "$text")
    user_agent=$(escape_json "$user_agent")

    cat << EOF
{"send_time": "$timestamp","data": {"action_id": "$action_id","action_name": "create_post","data": {"user_id": "$user_id","ip_address": "$ip_address","user_agent": "$user_agent","event_type": "create_post","post": {"text": "$text","embed_urls": [$embed_urls],"mentions": [$mentions],"extracted_urls": [$extracted_urls]},"account": {"created_at": "$account_created_at","post_count": $post_count,"follower_count": $follower_count}}}}
EOF
}

# Function to generate a send_message event
generate_send_message() {
    local is_spam=$1
    local message_text=""
    local user_agent=""
    local ip_address=""
    local post_count=$((RANDOM % 200))
    local follower_count=$((RANDOM % 1000))

    if [ "$is_spam" = "true" ]; then
        message_text="Hey! $(pick_random "${spam_words[@]}") Don't miss out! $(pick_random "${spam_words[@]}")"
        user_agent=$(pick_random "${suspicious_user_agents[@]}")
        ip_address=$(generate_ip_address suspicious)
        post_count=$((RANDOM % 5))
        follower_count=$((RANDOM % 3))
    else
        message_text="Hey ${handles[RANDOM % ${#handles[@]}]}, ${normal_words[RANDOM % ${#normal_words[@]}]} ${normal_words[RANDOM % ${#normal_words[@]}]}!"
        user_agent=$(pick_random "${user_agents[@]}")
        ip_address=$(generate_ip_address normal)
    fi

    local timestamp=$(generate_timestamp)
    local user_id=$(generate_random_user_id)
    local recipient_id=$(generate_random_user_id)
    local account_created_at=$(generate_account_created_at)
    local action_id=$(generate_action_id)

    message_text=$(escape_json "$message_text")
    user_agent=$(escape_json "$user_agent")

    cat << EOF
{"send_time": "$timestamp","data": {"action_id": "$action_id","action_name": "send_message","data": {"user_id": "$user_id","ip_address": "$ip_address","user_agent": "$user_agent","event_type": "send_message","message": {"text": "$message_text","recipient_id": "$recipient_id"},"account": {"created_at": "$account_created_at","post_count": $post_count,"follower_count": $follower_count}}}}
EOF
}

# Function to generate an identity_change event
generate_identity_change() {
    local is_suspicious=$1
    local change_type=""
    local previous_handle=""
    local new_handle=""
    local display_name=""
    local user_agent=""
    local ip_address=""
    local post_count=$((RANDOM % 200))
    local follower_count=$((RANDOM % 1000))

    # Randomly choose between handle and display_name change
    if [ $((RANDOM % 2)) -eq 0 ]; then
        change_type="handle"
    else
        change_type="display_name"
    fi

    previous_handle=$(pick_random "${handles[@]}")_$(random_in_range 100 999)

    if [ "$is_suspicious" = "true" ]; then
        if [ "$change_type" = "handle" ]; then
            new_handle=$(pick_random "${spam_handles[@]}")
        else
            new_handle="user_$(random_in_range 1000 9999)"
        fi
        display_name=$(pick_random "${spam_display_names[@]}")
        user_agent=$(pick_random "${suspicious_user_agents[@]}")
        ip_address=$(generate_ip_address suspicious)
        post_count=$((RANDOM % 10))
        follower_count=$((RANDOM % 5))
    else
        new_handle=$(pick_random "${handles[@]}")_$(random_in_range 1000 9999)
        display_name=$(pick_random "${display_names[@]}")
        user_agent=$(pick_random "${user_agents[@]}")
        ip_address=$(generate_ip_address normal)
    fi

    local timestamp=$(generate_timestamp)
    local user_id=$(generate_random_user_id)
    local account_created_at=$(generate_account_created_at)
    local action_id=$(generate_action_id)

    display_name=$(escape_json "$display_name")
    user_agent=$(escape_json "$user_agent")

    cat << EOF
{"send_time": "$timestamp","data": {"action_id": "$action_id","action_name": "identity_change","data": {"user_id": "$user_id","ip_address": "$ip_address","user_agent": "$user_agent","event_type": "identity_change","identity": {"previous_handle": "$previous_handle","new_handle": "$new_handle","display_name": "$display_name","change_type": "$change_type"},"account": {"created_at": "$account_created_at","post_count": $post_count,"follower_count": $follower_count}}}}
EOF
}

# Function to generate a random event
generate_random_event() {
    local event_type=$((RANDOM % 100))
    local is_malicious="false"

    # 20% chance of generating malicious content
    if [ $((RANDOM % 5)) -eq 0 ]; then
        is_malicious="true"
    fi

    if [ $event_type -lt 50 ]; then
        # 50% create_post
        generate_create_post "$is_malicious"
    elif [ $event_type -lt 80 ]; then
        # 30% send_message
        generate_send_message "$is_malicious"
    else
        # 20% identity_change
        generate_identity_change "$is_malicious"
    fi
}

# Function to send message to Kafka
send_to_kafka() {
    local message="$1"

    if [ "$USE_DOCKER" = "true" ]; then
        # Use Docker to send to Kafka (for local development)
        echo "$message" | docker exec -i osprey-kafka kafka-console-producer \
            --broker-list "$KAFKA_BROKER" \
            --topic "$KAFKA_TOPIC" 2>/dev/null
    else
        # Direct Kafka access (for production or external Kafka)
        local cmd="kafka-console-producer --broker-list $KAFKA_BROKER --topic $KAFKA_TOPIC"
        if [ -n "$KAFKA_CONFIG_FILE" ]; then
            cmd="$cmd --producer.config $KAFKA_CONFIG_FILE"
        fi
        echo "$message" | $cmd
    fi
}

# Initialize display counter for logging
event_count=0

# Function to handle cleanup on script termination
cleanup() {
    echo
    echo "Stopping demo data generation..."
    echo "Generated $event_count events"
    exit 0
}

# Set up signal handlers for graceful shutdown
trap cleanup SIGINT SIGTERM

# Check if Docker is available when USE_DOCKER=true
if [ "$USE_DOCKER" = "true" ]; then
    if ! docker ps | grep -q osprey-kafka; then
        echo "ERROR: osprey-kafka container is not running."
        echo "Please start Osprey with: docker compose up -d"
        echo "Or set USE_DOCKER=false to use external Kafka."
        exit 1
    fi
fi

# Reset Druid supervisor to ensure Kafka offsets are in sync
# This fixes the common issue where events appear in logs but not in the UI
echo "Resetting Druid supervisor to sync Kafka offsets..."
curl -s -X POST "http://localhost:8081/druid/indexer/v1/supervisor/osprey.execution_results/reset" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "Druid supervisor reset successfully."
else
    echo "Warning: Could not reset Druid supervisor (Druid may not be running yet)."
fi
echo

# Print banner
echo "=========================================="
echo "  Osprey Demo Data Generator"
echo "=========================================="
echo
echo "Generating mixed events to Kafka topic '$KAFKA_TOPIC'..."
echo "Kafka broker: $KAFKA_BROKER"
echo "Using Docker: $USE_DOCKER"
if [ -n "$KAFKA_CONFIG_FILE" ]; then
    echo "Using config file: $KAFKA_CONFIG_FILE"
fi
echo
echo "Event types: create_post (50%), send_message (30%), identity_change (20%)"
echo "Malicious content rate: ~20%"
echo
echo "Press Ctrl+C to stop..."
echo

# Infinite loop to generate and send actions
while true; do
    ((event_count++))
    action=$(generate_random_event)
    echo -e "[$event_count] Sending event..."
    send_to_kafka "$action"

    # Random delay between 0.5 and 2 seconds for realistic traffic
    sleep_time=$(awk "BEGIN {printf \"%.1f\", 0.5 + rand() * 1.5}")
    sleep "$sleep_time"
done
