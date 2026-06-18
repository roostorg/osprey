#!/bin/bash
# TEMP DIAGNOSTIC (do not merge): bring up the real test_runner entrypoint plus
# an external py-spy sidecar, wait for the sidecar to finish its observation,
# then surface the logs (which contain the native dump if the hang reproduced).
set -x
CF="-f docker-compose.yaml -f docker-compose.test.yaml -f docker-compose.diag.yaml --profile test"

docker compose $CF up -d --build test_runner pyspy

SPY=$(docker compose $CF ps -q pyspy)
echo "pyspy container: $SPY"
timeout 600 docker wait "$SPY" || true

echo "===================== test_runner logs ====================="
docker compose $CF logs --no-color --no-log-prefix test_runner
echo "===================== pyspy logs ====================="
docker compose $CF logs --no-color --no-log-prefix pyspy

docker compose $CF down -v --remove-orphans || true
