#!/bin/bash
# TEMP DIAGNOSTIC (do not merge): start the real test_runner entrypoint detached,
# then run the external py-spy sidecar in the FOREGROUND (it blocks until it has
# observed/dumped), then surface test_runner logs.
set -x
CF="-f docker-compose.yaml -f docker-compose.test.yaml -f docker-compose.diag.yaml --profile test"

docker compose $CF up -d --build test_runner

echo "===================== pyspy sidecar (foreground) ====================="
docker compose $CF run --rm pyspy

echo "===================== test_runner logs ====================="
docker compose $CF logs --no-color --no-log-prefix test_runner

docker compose $CF down -v --remove-orphans || true
