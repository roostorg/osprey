#!/bin/bash
set -euo pipefail

if [[ "${1:-}" != "run-tests" ]]; then
    echo "Usage: run-tests-with-coverage.sh run-tests [pytest arguments...]" >&2
    exit 2
fi

shift
exec uv run --locked --group coverage python3.11 -m gevent.monkey --module pytest "$@"
