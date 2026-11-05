#!/bin/bash
# Shared helpers for the devcontainer lifecycle scripts. Sourced, not run.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT" || exit 1

# devcontainer.json sets these; default them so the scripts also work when run by hand.
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-osprey}"
export COMPOSE_FILE="${COMPOSE_FILE:-$REPO_ROOT/docker-compose.yaml:$REPO_ROOT/.devcontainer/docker-compose.codespaces.yaml}"
export COMPOSE_PROFILES="${COMPOSE_PROFILES:-test_data}"
export DOCKER_CLI_HINTS=false

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

START_SECONDS=$SECONDS

step() { echo -e "${YELLOW}▶ $*${NC}"; }
ok() { echo -e "${GREEN}✓ $*${NC} ${BLUE}($((SECONDS - START_SECONDS))s)${NC}"; }
fail() { echo -e "${RED}✗ $*${NC}"; }

# The address people open in their browser. In a codespace that's the forwarded
# *.app.github.dev URL; anywhere else it's localhost.
ui_url() {
    if [ -n "${CODESPACE_NAME:-}" ]; then
        echo "https://${CODESPACE_NAME}-5002.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-app.github.dev}/"
    else
        echo "http://localhost:5002/"
    fi
}

# Every probe has its own time limit, so a hung service can't stall wait_for past its deadline.
docker_ready() { timeout 10 docker info >/dev/null 2>&1; }

wait_for_docker() {
    local attempt
    for attempt in $(seq 1 60); do
        docker_ready && return 0
        sleep 2
    done
    fail "Docker didn't start inside the codespace."
    return 1
}

# Most images come from Docker Hub, which rate-limits anonymous pulls per IP address.
# Signing in (from Codespaces secrets, if you've set them) avoids the limit.
docker_hub_login() {
    if [ -n "${DOCKERHUB_USERNAME:-}" ] && [ -n "${DOCKERHUB_TOKEN:-}" ]; then
        echo "$DOCKERHUB_TOKEN" | docker login --username "$DOCKERHUB_USERNAME" --password-stdin >/dev/null \
            && ok "Signed in to Docker Hub as $DOCKERHUB_USERNAME" \
            || fail "Couldn't sign in to Docker Hub as $DOCKERHUB_USERNAME; downloading anonymously."
    fi
    return 0
}

# wait_for <description> <timeout seconds> <command...>
wait_for() {
    local what=$1 timeout=$2
    shift 2
    local deadline=$((SECONDS + timeout))
    step "Waiting for $what..."
    until "$@" >/dev/null 2>&1; do
        if [ $SECONDS -ge $deadline ]; then
            fail "$what isn't ready after ${timeout}s."
            return 1
        fi
        sleep 3
    done
    ok "Ready: $what"
}

probe() { curl -fsS --max-time 10 "$@"; }
api_ready() { probe http://localhost:5004/_health; }
ui_ready() { probe http://localhost:5002/ && probe http://localhost:5002/api/config; }

# True once Druid has ingested an event from the last few minutes, i.e. data is flowing
# now (after a resume, older events are already there).
events_ready() {
    local count
    count=$(probe -X POST -H 'Content-Type: application/json' \
        -d '{"query":"SELECT COUNT(*) AS n FROM \"osprey.execution_results\" WHERE __time > CURRENT_TIMESTAMP - INTERVAL '"'"'5'"'"' MINUTE","resultFormat":"array"}' \
        http://localhost:8082/druid/v2/sql 2>/dev/null | tr -dc '0-9')
    [ -n "$count" ] && [ "$count" -gt 0 ]
}

# The UI with yesterday's and today's events already queried, grouped by user. Whole days,
# so the link keeps showing new events for the rest of the day after it's printed.
demo_url() {
    local start end
    start=$(date -u -d yesterday +%Y-%m-%dT00:00:00.000Z)
    end=$(date -u -d tomorrow +%Y-%m-%dT00:00:00.000Z)
    echo "$(ui_url)?start=${start}&end=${end}&interval=day&queryFilter=&topn=UserId"
}

# The long-running services the demo's data flows through: producer → Kafka → worker → Druid.
# Druid also needs ZooKeeper, Postgres (metadata) and MinIO (deep storage).
PIPELINE_SERVICES=(osprey-kafka-test-data-producer osprey-kafka osprey-worker
    druid-coordinator druid-broker druid-middlemanager druid-historical
    druid-zookeeper postgres minio)

# Prints the pipeline services that aren't running; prints nothing when they all are.
stopped_pipeline_services() {
    local running svc
    running=$(timeout 20 docker compose ps --status running --services 2>/dev/null)
    for svc in "${PIPELINE_SERVICES[@]}"; do
        grep -qx "$svc" <<<"$running" || echo "$svc"
    done
}

# Shown instead of the ready banner when the UI is up but no new events are arriving.
print_no_data_banner() {
    local stopped
    stopped=$(stopped_pipeline_services)
    echo
    echo -e "${YELLOW}${BOLD}The Osprey UI is up, but no new demo events are arriving.${NC}"
    echo -e "  UI: ${BOLD}$(ui_url)${NC}"
    if [ -n "$stopped" ]; then
        echo "  These services aren't running: $(echo $stopped)"
        echo "  Try:  docker compose up -d   (or rebuild the codespace; see the guide)"
    else
        echo "  Every service is running, so Druid may still be starting. Give it a few"
        echo "  minutes, then run:  bash .devcontainer/scripts/status.sh"
    fi
    echo "  Guide: docs/development/codespaces.md#troubleshooting"
    echo
}

print_ready_banner() {
    echo
    echo -e "${GREEN}${BOLD}==============================================${NC}"
    echo -e "${GREEN}${BOLD}  Osprey is ready${NC}"
    echo -e "${GREEN}${BOLD}==============================================${NC}"
    echo
    echo -e "  Open the UI:  ${BOLD}$(ui_url)${NC}"
    echo -e "  (or the ${BOLD}Ports${NC} tab → ${BOLD}Osprey UI${NC} → globe icon)"
    echo -e "  Then click ${BOLD}Submit Query${NC} to load the last day of events."
    echo
    echo "  Or open it with yesterday's and today's events already queried:"
    echo "  $(demo_url)"
    echo
    echo "  Synthetic posts arrive about once a second. Posts containing"
    echo "  \"hello\" fire the ContainsHello rule. To see only those, query:"
    echo -e "  ${BOLD}RegexMatch(target=PostText, pattern=\"hello\")${NC}"
    echo
    echo "  Guide: docs/development/codespaces.md"
    echo
}
