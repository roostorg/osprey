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
# deleted or renamed .proto would otherwise linger. Compute the set of files
# protoc writes for the current protos (it maps `-` to `_` in the module path)
# and remove any generated file outside that set.
# Kept as a plain file so the check behaves the same under bash and zsh.
expected_output=$(mktemp)
trap 'rm -f "$expected_output"' EXIT
for proto in "${glob[@]}"; do
    stem=${proto#./proto/}
    stem=${stem%.proto}
    stem=${stem//-/_}
    printf '%s\n' "$out_dir/${stem}_pb2.py" "$out_dir/${stem}_pb2.pyi" "$out_dir/${stem}_pb2_grpc.py"
done > "$expected_output"
find "$out_dir/osprey" -type f \( -name '*_pb2.py' -o -name '*_pb2.pyi' -o -name '*_pb2_grpc.py' \) | while read -r generated; do
    if ! grep -Fxq -- "$generated" "$expected_output"; then
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
