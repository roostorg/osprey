#!/bin/bash
# postAttachCommand: tell whoever just connected what state the demo is in.
source "$(dirname "$0")/lib.sh"

if ! docker info >/dev/null 2>&1 || ! api_ready >/dev/null 2>&1 || ! ui_ready >/dev/null 2>&1; then
    echo -e "${YELLOW}${BOLD}The Osprey demo is still starting.${NC}"
    echo "Progress is shown in the \"postStartCommand\" terminal (or View Creation Log)."
    echo -e "When it's ready, the UI opens at ${BOLD}$(ui_url)${NC}"
    exit 0
fi
print_ready_banner
