"""Builds the redacted effective-configuration dump printed by `osprey-cli config dump`.

The output shape is shared with coop's config dump:

    {
      "schema_version": 1,
      "generated_at": "2026-10-27T12:00:00+00:00",
      "app": {"name": "osprey", "version": "...", "commit": "<sha or null>"},
      "runtime": {"python": "3.11.9", "platform": "Linux-...-x86_64"},
      "plugins": ["stdlib", ...],
      "config": {"<component>": {"<KEY>": {"value": ..., "set": true, "source": "env", "redacted": true}}}
    }

`set` says whether the key is present in the environment; `source` is `env`, `default` (the code's literal
fallback) or `derived` (a fallback computed at runtime). `redacted` is only present when something was masked.
"""

import json
import platform
import subprocess
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

import yaml
from osprey.worker.lib.config.catalog import CATALOG, ConfigKey
from osprey.worker.lib.config.redaction import redact_entry

SCHEMA_VERSION = 1
PLUGIN_ENTRY_POINT_GROUPS = ('osprey_plugin', 'osprey_async_plugin')


def _parse_env_value(raw: str) -> Any:
    """Parses a value the way `config_from_env` does, but keeps the raw string instead of failing on bad JSON."""
    if raw.startswith(('{', '[', '"')):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw
    return raw


def config_entry(key: ConfigKey, env: Mapping[str, str]) -> dict[str, Any]:
    if key.name in env:
        value, is_set, source = _parse_env_value(env[key.name]), True, 'env'
    elif key.derived_default is not None:
        value, is_set, source = key.derived_default(env), False, 'derived'
    else:
        value, is_set, source = key.default, False, 'default'

    redacted_value, redacted = redact_entry(key.name, value, key.sensitive)
    entry: dict[str, Any] = {'value': redacted_value, 'set': is_set, 'source': source}
    if redacted:
        entry['redacted'] = True
    return entry


def build_config(
    env: Mapping[str, str], components: Iterable[str], catalog: Iterable[ConfigKey] = CATALOG
) -> dict[str, dict[str, dict[str, Any]]]:
    wanted = set(components)
    by_component: dict[str, dict[str, dict[str, Any]]] = {c: {} for c in sorted(wanted)}
    for key in catalog:
        for component in key.components:
            if component in wanted:
                by_component[component][key.name] = config_entry(key, env)
    return {c: dict(sorted(keys.items())) for c, keys in by_component.items()}


def parse_env_file(content: str) -> dict[str, str]:
    """Parses `KEY=VALUE` lines (`env`, docker env-files) or NUL-separated entries (`/proc/<pid>/environ`)."""
    nul_separated = '\0' in content
    env: dict[str, str] = {}
    for entry in content.split('\0') if nul_separated else content.splitlines():
        line = entry if nul_separated else entry.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        if line.startswith('export '):
            line = line[len('export ') :]
        name, _, value = line.partition('=')
        env[name.strip()] = value
    return env


def enabled_plugins() -> list[str]:
    """Names of the plugins registered through setuptools entry points, which is what pluggy loads."""
    names = {ep.name for group in PLUGIN_ENTRY_POINT_GROUPS for ep in metadata.entry_points(group=group)}
    return sorted(names)


def _distribution_version(name: str) -> str | None:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def app_version() -> str | None:
    """The installed package's version, which release prep sets to the release's tag."""
    return _distribution_version('osprey-worker')


def app_commit(env: Mapping[str, str]) -> str | None:
    """`OSPREY_COMMIT` if the deployment sets it, else the commit of the source checkout Osprey runs from."""
    return env.get('OSPREY_COMMIT') or git_commit()


def git_commit() -> str | None:
    """The commit of the source checkout, if Osprey runs from one (the dev stack mounts the repo)."""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    commit = result.stdout.strip()
    return commit if result.returncode == 0 and commit else None


def build_dump(
    env: Mapping[str, str],
    components: Iterable[str],
    *,
    now: datetime | None = None,
    version: str | None = None,
    commit: str | None = None,
    plugins: list[str] | None = None,
) -> dict[str, Any]:
    return {
        'schema_version': SCHEMA_VERSION,
        'generated_at': (now or datetime.now(UTC)).isoformat(timespec='seconds'),
        'app': {'name': 'osprey', 'version': version, 'commit': commit},
        'runtime': {'python': platform.python_version(), 'platform': platform.platform()},
        'plugins': plugins if plugins is not None else enabled_plugins(),
        'config': build_config(env, components),
    }


def render(dump: Mapping[str, Any], output_format: str) -> str:
    if output_format == 'yaml':
        return yaml.safe_dump(dict(dump), sort_keys=True, default_flow_style=False)
    return json.dumps(dump, sort_keys=True, indent=2) + '\n'
