#!/bin/bash

set -euo pipefail

exec make test PYTEST_ARGS="$*"
