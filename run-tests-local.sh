#!/bin/bash

# Run the Python tests on the host, against an already-running dev stack.
#
# Contrast ./run-tests.sh, which runs them inside a container in an isolated compose
# project -- that is what CI does, and it is the one to use when you want the tests to
# be unaffected by local state. This script is for a fast edit-run loop: it reuses the
# stack you already have up and talks to its exposed ports.
#
# The database is `osprey_test`, NOT the `osprey` one the dev stack runs on. The
# session fixture in lib/tests/test_utils.py drops the database it created, so pointing
# this at `osprey` would delete your development data and leave osprey-ui-api unable to
# start. The fixture refuses to drop a database that isn't named `*_test`, but there is
# no reason to rely on that.
#
# Usage: ./run-tests-local.sh [pytest args...]
#   ./run-tests-local.sh osprey_worker/src/osprey/worker/ui_api -q

set -euo pipefail

export PYTHONPATH=.
export TESTING=true
export POSTGRES_HOSTS='{"osprey_db":"postgresql://osprey:FoolishPassword@127.0.0.1:5432/osprey_test"}'
export SNOWFLAKE_API_ENDPOINT=http://127.0.0.1:8088
export SNOWFLAKE_EPOCH=1420070400000
export OSPREY_RULES_PATH=./example_rules
export OSPREY_DISABLE_VALIDATION_EXPORTER=true
export OSPREY_EXECUTION_RESULT_STORAGE_BACKEND=minio
export OSPREY_MINIO_ENDPOINT=127.0.0.1:9000
export OSPREY_MINIO_ACCESS_KEY=minioadmin
export OSPREY_MINIO_SECRET_KEY=minioadmin123
export OSPREY_MINIO_SECURE=false
export OSPREY_MINIO_EXECUTION_RESULTS_BUCKET=execution-output

exec uv run pytest "${@}"
