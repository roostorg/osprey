#!/bin/bash
# postAttachCommand for the development devcontainer: a reminder of how to run things.
cat <<'EOF'
Osprey development container. Nothing is running yet; start what you need:

  bash .devcontainer/scripts/start.sh   full stack with demo data, UI on port 5002
  ./run-tests.sh                         integration tests
  uv run pre-commit run --all-files      lint and type checks

See .devcontainer/README.md for more.
EOF
