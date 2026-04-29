#!/bin/bash

set -euo pipefail

exec make clickhouse-test PYTEST_ARGS="$*"
