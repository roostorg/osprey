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

out_dir=osprey_rpc/src

glob=(./proto/osprey/**/*.proto)
# Generate protobuf files
uv run -m grpc_tools.protoc --proto_path=proto --python_out="$out_dir" --mypy_out="$out_dir" --grpc_python_out="$out_dir" "${glob[@]}"

# protoc only writes outputs for the protos it is given, so bindings for a
# deleted or renamed .proto would otherwise linger. Remove any generated file
# whose source proto no longer exists.
find "$out_dir/osprey" -type f \( -name '*_pb2.py' -o -name '*_pb2.pyi' -o -name '*_pb2_grpc.py' \) | while read -r generated; do
    source_stem=${generated#"$out_dir"/}
    source_stem=${source_stem%_pb2_grpc.py}
    source_stem=${source_stem%_pb2.pyi}
    source_stem=${source_stem%_pb2.py}
    if [[ ! -f "proto/$source_stem.proto" ]]; then
        echo "Removing stale $generated"
        rm "$generated"
    fi
done

# Remove package directories that no longer hold any generated file.
# Bytecode caches do not count as content.
find "$out_dir/osprey/rpc" -mindepth 1 -depth -type d ! -name '__pycache__' ! -path '*/__pycache__/*' | while read -r package_dir; do
    if [[ -z "$(find "$package_dir" -type f ! -name '__init__.py' ! -path '*/__pycache__/*' -print -quit)" ]]; then
        echo "Removing empty package $package_dir"
        rm -r "$package_dir"
    fi
done
