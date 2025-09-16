#!/usr/bin/env bash
set -e
# osx has an old version of bash and won't allow globstars.
# Check if we're on macOS and not already running zsh
if [[ "$OSTYPE" == "darwin"* ]] && [[ -z "$ZSH_VERSION" ]] && command -v zsh >/dev/null 2>&1; then
    echo "Detected macOS, switching to zsh..."
    exec zsh "$0" "$@"
fi

shopt -s globstar 2>/dev/null || true
# change to directory of script
cd "$(dirname "$0")"

glob=(./proto/**/*.proto)
# Generate protobuf files
uv run -m grpc_tools.protoc --proto_path=proto --python_out=osprey_rpc/src --mypy_out=osprey_rpc/src --grpc_python_out=osprey_rpc/src "${glob[@]}"
