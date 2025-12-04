#!/bin/bash
set -e

# Helper script to start Osprey with different configurations
# Usage:
#   ./start.sh                    # Start with worker directly consuming from Kafka
#   ./start.sh --with-coordinator # Start with Osprey Coordinator
#   ./start.sh --help             # Show this help

show_help() {
    echo "Osprey Startup Helper"
    echo ""
    echo "Usage: ./start.sh [OPTIONS] [COMPOSE_ARGS...]"
    echo ""
    echo "Options:"
    echo "  --with-coordinator    Start Osprey with Coordinator (workers connect to coordinator)"
    echo "  --help, -h            Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./start.sh                                    # Direct Kafka consumption"
    echo "  ./start.sh --with-coordinator                 # With coordinator"
    echo "  ./start.sh --with-coordinator up -d           # With coordinator in detached mode"
    echo "  ./start.sh --with-coordinator --profile test_data up  # With test data producer"
    echo ""
    echo "When using --with-coordinator, the following services are added:"
    echo "  - osprey-coordinator: Action distribution and load balancing"
    echo "  - etcd: Service discovery for coordinator"
    echo ""
}

USE_COORDINATOR=false
COMPOSE_FILES="-f docker-compose.yaml"
COMPOSE_ARGS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --with-coordinator)
            USE_COORDINATOR=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            # Pass remaining args to docker compose
            COMPOSE_ARGS+=("$1")
            shift
            ;;
    esac
done

if [ "$USE_COORDINATOR" = true ]; then
    echo "Starting Osprey with Coordinator..."
    COMPOSE_FILES="$COMPOSE_FILES -f example_docker_compose/run_osprey_with_coordinator/docker-compose.coordinator.yaml"
else
    echo "Starting Osprey without Coordiantor (direct Kafka consumption)..."
fi

# If no compose args provided, default to 'up'
if [ ${#COMPOSE_ARGS[@]} -eq 0 ]; then
    COMPOSE_ARGS=("up")
fi

echo "Running: docker compose $COMPOSE_FILES ${COMPOSE_ARGS[@]}"
echo ""

exec docker compose $COMPOSE_FILES "${COMPOSE_ARGS[@]}"
