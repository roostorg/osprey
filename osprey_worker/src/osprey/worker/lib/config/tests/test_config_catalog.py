"""Keeps `catalog.py` in sync with the code: every config key read anywhere must be catalogued, or
`osprey-config dump` silently omits it.

The Python side is scanned with `ast`; the coordinator (Rust) and UI with regexes. Sources outside
`osprey_worker` are skipped when absent (the integration-test container only mounts `osprey_worker`).
"""

import ast
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from osprey.worker.lib.config.catalog import ALL_COMPONENTS, CATALOG, COORDINATOR, PYTHON_COMPONENTS, UI

REPO_ROOT = Path(__file__).resolve().parents[7]
PYTHON_SOURCE_ROOTS = [REPO_ROOT / 'osprey_worker' / 'src', REPO_ROOT / 'osprey_async_worker' / 'src']

_CONFIG_METHODS = {
    f'{prefix}_{kind}' for prefix in ('get', 'expect') for kind in ('str', 'float', 'bool', 'int', 'type', 'str_list')
} | {'get_optional_str'}

# Reads whose key isn't a literal, by file and key expression. Their keys are catalogued by hand.
KNOWN_DYNAMIC_READS = {
    ('osprey/worker/lib/etcd/__init__.py', 'env_variable'),  # ETCD_PEERS / ETCD_SECURE_PEERS
    ('osprey/worker/lib/etcd/__init__.py', "'{}_WATCHERD_ADDR'.format(env_variable)"),
    ('osprey/worker/lib/osprey_logging/__init__.py', 'key'),  # environ_bool() itself
    ('osprey/worker/lib/config/__init__.py', 'key'),  # Config's own accessors
}

# Keys whose call sites disagree on the default; the catalog documents the one the main code path uses.
KNOWN_VARYING_DEFAULTS = {
    'DD_SERVICE',  # 'unknown' (tracing), '' (gc metrics), 'osprey-api' (pyroscope)
    'OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID',  # platform.node() for the rules sink, 'localhost' for other sinks
    'ENVIRONMENT',  # unset everywhere except bulk action uploads, which assume 'development'
}


def _is_os_environ(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == 'environ'
        and isinstance(node.value, ast.Name)
        and node.value.id == 'os'
    )


def _literal(node: ast.expr, constants: dict[str, Any]) -> Any:
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    return ast.literal_eval(node)


def _module_constants(tree: ast.Module) -> dict[str, Any]:
    constants: dict[str, Any] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            try:
                constants[node.targets[0].id] = ast.literal_eval(node.value)
            except ValueError:
                pass
    return constants


def _config_reads(tree: ast.Module) -> Iterator[tuple[ast.expr, ast.expr | None]]:
    """Yields `(key_expr, default_expr)` for each config or environment read."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and node.args:
            func = node.func
            default = node.args[1] if len(node.args) > 1 else None
            if isinstance(func, ast.Attribute) and func.attr in _CONFIG_METHODS:
                yield node.args[0], (default if func.attr.startswith('get_') else None)
            elif isinstance(func, ast.Attribute) and func.attr == 'get' and _is_os_environ(func.value):
                yield node.args[0], default
            elif (
                isinstance(func, ast.Attribute)
                and func.attr == 'getenv'
                and isinstance(func.value, ast.Name)
                and func.value.id == 'os'
            ):
                yield node.args[0], default
            elif isinstance(func, ast.Name) and func.id == 'environ_bool':
                yield node.args[0], default
        elif isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load):
            value = node.value
            is_config = isinstance(value, ast.Name) and value.id == 'config'
            if is_config or _is_os_environ(value):
                yield node.slice, None


def _python_reads() -> Iterator[tuple[str, str, Any]]:
    """Yields `(relative_path, key, default)`; `key` is the unparsed expression when it isn't a literal, and
    `default` is `...` when there is none or it isn't a literal."""
    for root in PYTHON_SOURCE_ROOTS:
        if not root.exists():
            continue
        for path in sorted(root.rglob('*.py')):
            if 'tests' in path.parts or path.name.startswith('test_') or path.name == 'conftest.py':
                continue
            tree = ast.parse(path.read_text(), str(path))
            constants = _module_constants(tree)
            relative = path.relative_to(root).as_posix()
            for key_expr, default_expr in _config_reads(tree):
                try:
                    key = _literal(key_expr, constants)
                except ValueError:
                    yield relative, ast.unparse(key_expr), ...
                    continue
                if not isinstance(key, str):
                    continue
                try:
                    default = ... if default_expr is None else _literal(default_expr, constants)
                except ValueError:
                    default = ...
                yield relative, key, default


def _catalogued(components: tuple[str, ...]) -> dict[str, list[Any]]:
    keys: dict[str, list[Any]] = {}
    for key in CATALOG:
        if set(key.components) & set(components):
            keys.setdefault(key.name, []).append(key.default)
    return keys


def test_python_reads_are_catalogued() -> None:
    catalogued = _catalogued(PYTHON_COMPONENTS)
    reads = list(_python_reads())
    assert reads, 'scanned no sources'

    missing = sorted({key for _, key, _ in reads if key.isupper() and key not in catalogued})
    assert not missing, f'add these keys to osprey/worker/lib/config/catalog.py: {missing}'

    dynamic = {(path, key) for path, key, _ in reads if not re.fullmatch(r'[A-Z][A-Z0-9_]*', key)}
    assert dynamic == KNOWN_DYNAMIC_READS, 'new non-literal config reads: catalogue their keys by hand'


def test_python_defaults_match_catalog() -> None:
    catalogued = _catalogued(PYTHON_COMPONENTS)
    mismatches = sorted(
        (key, default, catalogued[key])
        for path, key, default in _python_reads()
        if default is not ... and key in catalogued and key not in KNOWN_VARYING_DEFAULTS
        if default not in catalogued[key]
    )
    assert not mismatches


def test_coordinator_reads_are_catalogued() -> None:
    src = REPO_ROOT / 'osprey_coordinator' / 'src'
    if not src.exists():
        pytest.skip('osprey_coordinator sources not available')
    pattern = re.compile(
        r'env::var\("([A-Z0-9_]+)"\)|env = "([A-Z0-9_]+)"|(?:from_environment|base_client_builder)\("([A-Z0-9_]+)"\)'
    )
    keys = {
        next(g for g in m.groups() if g)
        for path in src.rglob('*.rs')
        # Standalone etcd_watcherd tools; not part of the coordinator's config.
        if 'bin' not in path.relative_to(src).parts
        for m in pattern.finditer(path.read_text())
    }
    assert keys
    assert sorted(keys - set(_catalogued((COORDINATOR,)))) == []


def test_ui_reads_are_catalogued() -> None:
    src = REPO_ROOT / 'osprey_ui' / 'src'
    if not src.exists():
        pytest.skip('osprey_ui sources not available')
    pattern = re.compile(r'process\.env\.(REACT_APP_[A-Z0-9_]+)')
    keys = {m.group(1) for path in src.rglob('*.ts*') for m in pattern.finditer(path.read_text())}
    assert keys
    assert sorted(keys - set(_catalogued((UI,)))) == []


def test_catalog_is_well_formed() -> None:
    seen = set()
    for key in CATALOG:
        assert key.components and set(key.components) <= set(ALL_COMPONENTS), key
        assert not (key.default is not None and key.derived_default is not None), key
        for component in key.components:
            assert (component, key.name) not in seen, f'{key.name} listed twice for {component}'
            seen.add((component, key.name))
