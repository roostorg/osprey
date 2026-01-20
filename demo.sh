#!/bin/bash

# Osprey Demo Script
# This script starts all services, generates test data, and opens the UI
#
# Usage:
#   From repo:  ./demo.sh
#   Standalone: curl -sSL https://raw.githubusercontent.com/roostorg/osprey/main/demo.sh | bash

set -e

REPO_URL="https://github.com/roostorg/osprey.git"
DEMO_DIR="osprey-demo"
COMPOSE_FILE="docker-compose.yaml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_banner() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}       Osprey Demo Setup Script        ${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
}

check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"

    # Check for Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}✗ Docker is not installed. Please install Docker first.${NC}"
        echo "  Visit: https://docs.docker.com/get-docker/"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker found${NC}"

    # Check for Docker Compose v2
    if ! docker compose version &> /dev/null; then
        echo -e "${RED}✗ Docker Compose v2 is not available.${NC}"
        echo "  Please update Docker Desktop or install docker-compose-plugin"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker Compose found${NC}"

    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        echo -e "${RED}✗ Docker daemon is not running. Please start Docker.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker daemon is running${NC}"
    echo ""
}

check_port_available() {
    local port=$1
    local service=$2
    if lsof -i :"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo -e "${RED}✗ Port $port is already in use (needed for $service)${NC}"
        echo -e "  Run: lsof -i :$port  to see what's using it"
        return 1
    fi
    return 0
}

check_required_ports() {
    echo -e "${YELLOW}Checking required ports...${NC}"

    local failed=0

    # Infrastructure ports
    check_port_available 9092 "Kafka" || failed=1
    check_port_available 9000 "MinIO API" || failed=1
    check_port_available 9001 "MinIO Console" || failed=1
    check_port_available 5432 "PostgreSQL" || failed=1
    check_port_available 8088 "Snowflake ID Worker" || failed=1

    # Druid ports
    check_port_available 2181 "Zookeeper" || failed=1
    check_port_available 8081 "Druid Coordinator" || failed=1
    check_port_available 8082 "Druid Broker" || failed=1
    check_port_available 8083 "Druid Historical" || failed=1
    check_port_available 8091 "Druid MiddleManager" || failed=1
    check_port_available 8888 "Druid Router" || failed=1

    # Druid MiddleManager task ports
    for port in 8100 8101 8102 8103 8104 8105; do
        check_port_available $port "Druid MiddleManager Tasks" || failed=1
    done

    # Osprey service ports
    check_port_available 5001 "Osprey Worker" || failed=1
    check_port_available 5002 "Osprey UI" || failed=1
    check_port_available 5004 "Osprey UI API" || failed=1

    if [ $failed -eq 1 ]; then
        echo ""
        echo -e "${RED}✗ Some required ports are in use. Please free them before running the demo.${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ All required ports are available${NC}"
    echo ""
}

setup_repo() {
    # Check if we're already in the osprey repo
    if [ -f "docker-compose.yaml" ] && grep -q "osprey-worker" "docker-compose.yaml" 2>/dev/null; then
        echo -e "${GREEN}✓ Running from Osprey repository${NC}"
        return 0
    fi

    # Check if we need to clone
    echo -e "${YELLOW}Osprey repository not found. Setting up...${NC}"

    if ! command -v git &> /dev/null; then
        echo -e "${RED}✗ Git is not installed. Please install Git first.${NC}"
        exit 1
    fi

    if [ -d "$DEMO_DIR" ]; then
        echo -e "${YELLOW}Directory '$DEMO_DIR' already exists, using it${NC}"
        cd "$DEMO_DIR"
        echo -e "${YELLOW}Pulling latest changes...${NC}"
        git pull
        echo -e "${GREEN}✓ Repository updated${NC}"
        echo ""
        return 0
    else
        echo -e "${YELLOW}Cloning Osprey repository...${NC}"
        git clone --depth 1 "$REPO_URL" "$DEMO_DIR"
    fi

    cd "$DEMO_DIR"
    echo -e "${GREEN}✓ Repository ready${NC}"
    echo ""
}

# Run the functions we defined above
print_banner
check_prerequisites
check_required_ports
setup_repo

# Function to check if a service is healthy via HTTP
wait_for_http_service() {
    local service=$1
    local url=$2
    local max_attempts=${3:-60}
    local attempt=1

    echo -e "${YELLOW}Waiting for $service...${NC}"
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ $service is ready${NC}"
            return 0
        fi
        sleep 2
        attempt=$((attempt + 1))
    done
    echo -e "${RED}✗ $service failed to start${NC}"
    return 1
}

# Function to check if a Docker container is healthy
wait_for_container() {
    local container=$1
    local max_attempts=${2:-60}
    local attempt=1

    echo -e "${YELLOW}Waiting for $container...${NC}"
    while [ $attempt -le $max_attempts ]; do
        local status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "not_found")
        if [ "$status" = "healthy" ]; then
            echo -e "${GREEN}✓ $container is ready${NC}"
            return 0
        elif [ "$status" = "not_found" ]; then
            # Container doesn't have health check, check if running
            local running=$(docker inspect --format='{{.State.Running}}' "$container" 2>/dev/null || echo "false")
            if [ "$running" = "true" ]; then
                echo -e "${GREEN}✓ $container is running${NC}"
                return 0
            fi
        fi
        sleep 2
        attempt=$((attempt + 1))
    done
    echo -e "${RED}✗ $container failed to start${NC}"
    return 1
}

# Step 1: Stop any existing services and remove volumes for clean state
echo -e "${YELLOW}Step 1: Cleaning up existing services and volumes...${NC}"
DOCKER_CLI_HINTS=false docker compose -f "$COMPOSE_FILE" --progress=quiet --profile test_data down -v 2>/dev/null || true
DOCKER_CLI_HINTS=false docker compose --progress=quiet --profile test_data down -v 2>/dev/null || true  # Also clean up old full compose
echo -e "${GREEN}✓ Cleanup complete (volumes removed for fresh start)${NC}"
echo ""

# Step 2: Pull Docker images
echo -e "${YELLOW}Step 2: Pulling Docker images (this may take a few minutes on first run)...${NC}"
DOCKER_CLI_HINTS=false docker compose -f "$COMPOSE_FILE" --progress=quiet pull --quiet
echo -e "${GREEN}✓ Images pulled${NC}"
echo ""

# Step 3: Start core services (build if needed)
echo -e "${YELLOW}Step 3: Starting core services (building images if needed)...${NC}"
DOCKER_CLI_HINTS=false docker compose -f "$COMPOSE_FILE" --progress=quiet up -d --build --quiet-pull
echo -e "${GREEN}✓ Core services starting${NC}"
echo ""

# Step 4: Wait for critical services
echo -e "${YELLOW}Step 4: Waiting for services to be ready...${NC}"
echo ""

# Wait for infrastructure containers (use Docker health checks)
wait_for_container "kafka" 90 || exit 1
wait_for_container "postgres" 90 || exit 1
wait_for_container "minio" 90 || exit 1

# Wait for Druid broker (use HTTP check - no Docker health check configured)
wait_for_http_service "Druid Broker" "http://localhost:8082/status" 180 || exit 1

# Wait for osprey services (HTTP check)
wait_for_http_service "Osprey UI API" "http://localhost:5004/config" 120 || exit 1
wait_for_http_service "Osprey UI" "http://localhost:5002" 90 || exit 1

echo ""
echo -e "${GREEN}✓ All services are ready${NC}"
echo ""

# Step 5: Start test data generator
echo -e "${YELLOW}Step 5: Starting test data generator...${NC}"
DOCKER_CLI_HINTS=false docker compose -f "$COMPOSE_FILE" --progress=quiet --profile test_data up -d kafka-test-data-producer
echo -e "${GREEN}✓ Test data generator started (1 event/second)${NC}"
echo ""

# Step 6: Wait for some events to be processed
echo -e "${YELLOW}Step 6: Waiting for events to be processed...${NC}"
sleep 15
echo -e "${GREEN}✓ Events should now be in Druid${NC}"
echo ""

# Generate UI URL with date range
START_DATE=$(date -u -v-1d +%Y-%m-%dT00:00:00.000Z 2>/dev/null || date -u -d "yesterday" +%Y-%m-%dT00:00:00.000Z)
END_DATE=$(date -u +%Y-%m-%dT23:59:59.999Z 2>/dev/null || date -u +%Y-%m-%dT23:59:59.999Z)
UI_URL="http://localhost:5002/?start=${START_DATE}&end=${END_DATE}&interval=day&queryFilter=&topn=UserId"

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}       Demo Ready!                     ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${BLUE}Available Interfaces:${NC}"
echo -e "  • Osprey UI:      ${GREEN}${UI_URL}${NC}"
echo -e "  • Druid Console:  ${GREEN}http://localhost:8888${NC}"
echo -e "  • Osprey API:     ${GREEN}http://localhost:5004${NC}"
echo ""
echo -e "${BLUE}Demo Rules Active:${NC}"
echo -e "  • ContainsHello  - Bans users who say 'hello'"
echo -e "  • LazyPostRule   - Labels posts with 'lazy' as low_effort"
echo -e "  • QuickPostRule  - Labels posts with 'quick' as potential_bot"
echo -e "  • FoxPostRule    - Bans users who say 'fox' (spam pattern)"
echo ""
echo -e "${BLUE}What to Demo:${NC}"
echo -e "  1. Event Stream  - See processed events with rule matches"
echo -e "  2. TopN Panel    - See users grouped by labels/bans"
echo -e "  3. Timeseries    - See event volume over time"
echo -e "  4. Query Filter  - Try: LazyPostRule == True"
echo -e "  5. Rules Viz     - View the rule dependency graph"
echo ""
echo -e "${YELLOW}Opening Osprey UI in browser...${NC}"

# Open browser (works on macOS, Linux with xdg-open, or WSL)
if command -v open &> /dev/null; then
    open "$UI_URL"
elif command -v xdg-open &> /dev/null; then
    xdg-open "$UI_URL"
elif command -v wslview &> /dev/null; then
    wslview "$UI_URL"
else
    echo -e "${YELLOW}Please open this URL manually: ${UI_URL}${NC}"
fi

echo ""
echo -e "${BLUE}To stop the demo:${NC}"
echo -e "  docker compose --profile test_data down -v"
echo ""
echo -e "${GREEN}Demo is running! Press Ctrl+C to exit this script (services will keep running)${NC}"
