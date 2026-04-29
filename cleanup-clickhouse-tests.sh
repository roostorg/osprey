#!/bin/bash

set -euo pipefail

if [ "$#" -eq 0 ]; then
  exec make clickhouse-test-down
elif [ "$#" -eq 1 ] && [ "$1" = "--volumes" ]; then
  exec make clickhouse-test-reset
else
  echo "Usage: ./cleanup-clickhouse-tests.sh [--volumes]" >&2
  exit 1
fi
