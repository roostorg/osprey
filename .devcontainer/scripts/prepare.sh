#!/bin/bash
# updateContentCommand: download and build every image the demo needs, so that
# starting the stack later is quick. Runs once when the codespace (or its prebuild)
# is created, and again when a prebuild picks up new commits.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

echo -e "${BOLD}Preparing the Osprey demo. The first time, this takes several minutes.${NC}"
wait_for_docker

# Most images come from Docker Hub, which rate-limits anonymous pulls per IP address.
# Signing in (from Codespaces secrets, if you've set them) avoids the limit.
if [ -n "${DOCKERHUB_USERNAME:-}" ] && [ -n "${DOCKERHUB_TOKEN:-}" ]; then
    echo "$DOCKERHUB_TOKEN" | docker login --username "$DOCKERHUB_USERNAME" --password-stdin >/dev/null \
        && ok "Signed in to Docker Hub as $DOCKERHUB_USERNAME"
fi

pull_failed() {
    fail "Couldn't download or build the demo's images."
    echo "  If the error above mentions \"rate limit\" or \"toomanyrequests\", Docker Hub is"
    echo "  limiting anonymous downloads. Wait an hour and rebuild the codespace, or add"
    echo "  DOCKERHUB_USERNAME and DOCKERHUB_TOKEN as Codespaces secrets."
    echo "  More help: docs/development/codespaces.md#troubleshooting"
    exit 1
}

step "Downloading service images (Kafka, Druid, Postgres, ...)"
docker compose pull --ignore-buildable --quiet || pull_failed
ok "Images downloaded"

step "Building the Osprey worker, API and UI images"
docker compose build --quiet || pull_failed
ok "Images built"

echo -e "${GREEN}Preparation finished. The demo starts next.${NC}"
