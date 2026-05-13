#!/usr/bin/env python3
"""Emit a sorted name@version list from either lockfile format.

Usage:
    lockfile_parity.py npm  <path-to-package-lock.json>
    lockfile_parity.py pnpm <path-to-pnpm-lock.yaml>

Comparing the two outputs (e.g. via `diff`) surfaces drift between
the npm resolution baseline and the pnpm-imported resolution.
"""

import json
import re
import subprocess
import sys
from pathlib import Path


def npm_pairs(path: Path) -> set[str]:
    """Walk package-lock.json (lockfileVersion 3) and yield name@version."""
    data = json.loads(path.read_text())
    out: set[str] = set()
    for key, entry in (data.get("packages") or {}).items():
        if key == "":
            continue  # root project entry
        # key is "node_modules/<...>/<name>" — last segment is the package name.
        # Use the entry's own name/version where present (handles aliasing).
        name = entry.get("name") or key.rsplit("node_modules/", 1)[-1]
        version = entry.get("version")
        if not version:
            continue  # workspace links, links, etc.
        out.add(f"{name}@{version}")
    return out


def _load_yaml(path: Path) -> dict:
    try:
        import yaml  # type: ignore
        return yaml.safe_load(path.read_text()) or {}
    except ImportError:
        # Fallback: shell out to `yq` (cmd-line YAML parser).
        result = subprocess.run(
            ["yq", "-o=json", ".", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(result.stdout)


# pnpm 10 emits lockfileVersion 9.0, where packages: keys are
# plain "<name>@<ver>" or "@<scope>/<name>@<ver>" — NO leading slash.
# Older pnpm v6/v8 used "/<name>@<ver>" with a leading slash; the
# regex makes that slash optional for forward/backward compatibility.
# In v9, peer-doppelganger keys live in a separate `snapshots:`
# block and don't appear here at all — so the [^(_]+ stop class
# matters less but stays harmless.
_PKG_KEY = re.compile(
    r"^/?(?P<name>(?:@[^/]+/)?[^/@]+)@(?P<version>[^(_]+)"
)


def pnpm_pairs(path: Path) -> set[str]:
    data = _load_yaml(path)
    out: set[str] = set()
    for key in (data.get("packages") or {}).keys():
        m = _PKG_KEY.match(key)
        if not m:
            print(f"WARN: unparseable key in pnpm-lock.yaml: {key!r}",
                  file=sys.stderr)
            continue
        out.add(f"{m['name']}@{m['version']}")
    return out


def main() -> int:
    if len(sys.argv) != 3 or sys.argv[1] not in {"npm", "pnpm"}:
        print(__doc__, file=sys.stderr)
        return 2
    mode, path = sys.argv[1], Path(sys.argv[2])
    pairs = npm_pairs(path) if mode == "npm" else pnpm_pairs(path)
    for p in sorted(pairs):
        print(p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
