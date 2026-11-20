#!/bin/bash
# updateContentCommand: download and build every image the demo needs, so that
# starting the stack later is quick. Runs once when the codespace (or its prebuild)
# is created, and again when a prebuild picks up new commits.
#
# Failures here only warn: a failed updateContentCommand would stop postStartCommand
# (start.sh) from running at all, and start.sh's `docker compose up` downloads and builds
# whatever is still missing, reporting any error itself.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

echo -e "${BOLD}Preparing the Osprey demo. The first time, this takes several minutes.${NC}"
if ! wait_for_docker; then
    echo "  Skipping preparation; the demo tries again when it starts."
    exit 0
fi

docker_hub_login

pull_failed() {
    fail "Couldn't download or build the demo's images."
    echo "  If the error above mentions \"rate limit\" or \"toomanyrequests\", Docker Hub is"
    echo "  limiting anonymous downloads. Wait an hour and rebuild the codespace, or add"
    echo "  DOCKERHUB_USERNAME and DOCKERHUB_TOKEN as Codespaces secrets."
    echo "  More help: docs/development/codespaces.md#troubleshooting"
    echo "  The demo tries again when it starts."
    exit 0
}

step "Downloading service images (Kafka, Druid, Postgres, ...)"
docker compose pull --ignore-buildable --quiet || pull_failed
ok "Images downloaded"

step "Building the Osprey worker, API and UI images"
docker compose build --quiet || pull_failed
ok "Images built"

echo -e "${GREEN}Preparation finished. The demo starts next.${NC}"
