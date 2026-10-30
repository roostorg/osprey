#!/bin/bash
# postStartCommand: start the demo stack and wait until the UI has data to show.
# Runs on every start, including when a stopped codespace resumes.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

on_failure() {
    echo
    fail "The Osprey demo didn't start cleanly."
    echo "  Try: Ctrl+Shift+P (Cmd+Shift+P on a Mac) → \"Codespaces: Rebuild Container\"."
    echo "  To see which service is failing, run:  docker compose ps -a"
    echo "  More help: docs/development/codespaces.md#troubleshooting"
    exit 1
}

echo -e "${BOLD}Starting the Osprey demo. This takes 1-3 minutes.${NC}"
echo "The Osprey UI opens in a new browser tab once it's ready; this terminal"
echo "keeps showing progress."
echo

wait_for_docker || on_failure

step "Starting services"
docker compose up -d --quiet-pull || on_failure
ok "Services started"

wait_for "the Osprey API" 300 api_ready || on_failure
wait_for "the Osprey UI" 300 ui_ready || on_failure
# Druid needs a minute or two to start ingesting; the UI works meanwhile, its
# charts are just empty. A slow first event isn't a failure.
if ! wait_for "the first demo events in the charts" 300 events_ready; then
    echo "  The UI works, but its charts stay empty until events arrive."
    echo "  If they haven't after a few minutes, see the troubleshooting guide."
fi

print_ready_banner
