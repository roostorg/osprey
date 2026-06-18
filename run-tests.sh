#!/bin/bash
# TEMP DIAGNOSTIC (do not merge): run the EXACT real entrypoint via
# `docker compose run` (the config that hung 4/4), backgrounded + named, and
# attach an external py-spy sidecar to its PID namespace to capture the native
# stack of the hung process.
set -x
CF="-f docker-compose.yaml -f docker-compose.test.yaml -f docker-compose.diag.yaml --profile test"

# Pre-build images (test_runner + pyspy share the build).
docker compose $CF build test_runner pyspy

# Real entrypoint via `docker compose run`, backgrounded + named so the sidecar
# can join its PID namespace.
docker compose $CF run --rm --name osprey_tr test_runner run-tests &
RUNPID=$!

# Let deps boot and pytest start, then run the sidecar (it self-terminates after
# its post-suite observation window or when the shared PID namespace dies).
sleep 45
docker compose $CF run --rm --no-deps pyspy

wait "$RUNPID"
docker compose $CF down -v --remove-orphans || true
