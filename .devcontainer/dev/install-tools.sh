#!/bin/bash
# onCreateCommand for the development devcontainer: tools the devcontainer features don't
# cover. Versions follow .github/workflows/code-quality.yml.
set -euo pipefail

echo "▶ Installing uv"
curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_UNMANAGED_INSTALL=/usr/local/bin sh

echo "▶ Installing protoc (for osprey_coordinator)"
sudo apt-get update -qq
sudo apt-get install -y -qq protobuf-compiler

echo "▶ Enabling Corepack (for pnpm)"
npm install -g corepack@0.33.0
corepack enable
