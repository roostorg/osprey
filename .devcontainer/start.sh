#!/usr/bin/env bash
# postStartCommand: bring the Osprey stack back up on every Codespace (re)start.
#
# setup.sh (postCreateCommand) only runs once, on first create. When a Codespace
# suspends and resumes, the Docker stack is stopped and nothing restarts it, and
# Kafka (KRaft, no persistent volume) frequently comes back unhealthy after the
# ungraceful stop, which blocks the worker and ui-api. This heals Kafka if
# needed and brings the stack back up, so a reopen "just works".
#
# No -e: a transient failure here shouldn't fail the whole Codespace start.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 0

# Only the live-Jetstream path uses run-atproto.sh; mirror setup.sh's detection.
if [ ! -f run-atproto.sh ] || [ ! -f docker-compose.atproto.yaml ]; then
  exit 0
fi

kafka_healthy() {
  docker compose ps osprey-kafka 2>/dev/null | grep -q '(healthy)'
}

if ! kafka_healthy; then
  echo "==> osprey-kafka not healthy; recreating it fresh (KRaft has no volume)"
  docker compose up -d --force-recreate --no-deps osprey-kafka
  for _ in $(seq 1 24); do
    kafka_healthy && break
    sleep 5
  done
fi

echo "==> bringing up the Osprey stack"
# Point the UI at the forwarded API URL (same as setup.sh). Without this, the
# recreated osprey-ui defaults to localhost:5004 and the browser can't reach the
# API (getApplicationConfig "Network Error").
if [ -n "${CODESPACE_NAME:-}" ]; then
  export REACT_APP_API_BASE_URL="https://${CODESPACE_NAME}-5004.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-app.github.dev}"
fi
bash run-atproto.sh up -d || true

exit 0
