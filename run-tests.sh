#!/bin/bash
# See issue #368 for the teardown-hang the wrappers below bound.
set -euo pipefail

COMPOSE_FILES=(-f docker-compose.yaml -f docker-compose.test.yaml --profile test)
RUN_TESTS_TIMEOUT_SECONDS="${RUN_TESTS_TIMEOUT_SECONDS:-600}"

cleanup_on_failure() {
    local exit_code=$?
    if [ "${exit_code}" -ne 0 ]; then
        docker compose "${COMPOSE_FILES[@]}" ps -a 2>/dev/null || true
        timeout 60 docker compose "${COMPOSE_FILES[@]}" down --timeout 10 --volumes 2>/dev/null || true
    fi
}
trap cleanup_on_failure EXIT

exit_code=0
timeout "${RUN_TESTS_TIMEOUT_SECONDS}" \
    docker compose "${COMPOSE_FILES[@]}" run --rm test_runner run-tests "${@}" || exit_code=$?

if [ "${exit_code}" -eq 124 ]; then
    echo "::error::run-tests.sh exceeded ${RUN_TESTS_TIMEOUT_SECONDS}s (issue #368)" >&2
fi

exit "${exit_code}"
