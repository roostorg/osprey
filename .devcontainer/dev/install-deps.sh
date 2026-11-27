#!/bin/bash
# updateContentCommand for the development devcontainer: project dependencies and git hooks.
# Runs when the codespace is created or prebuilt, and again when its content updates.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

echo "▶ Installing Python dependencies"
uv sync --dev

echo "▶ Installing UI dependencies"
(cd osprey_ui && pnpm install --frozen-lockfile)

echo "▶ Installing git hooks"
uv run pre-commit install --install-hooks
