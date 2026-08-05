"""Schema loader for typed action contracts.

Loads per-action JSON schemas from a smite-rules checkout.
Schema format is defined in §4.3 of the typed-action-contracts plan.
"""

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Set

log = logging.getLogger(__name__)

_SUPPORTED_VERSION = 1

# Per-action allowlists (env), so rollout is action-by-action with an env-only kill
# switch. Both default OFF — a wrong `absent` entry silently drops features/enforcement,
# so schema files on the rules path do nothing until an action is explicitly listed.
# PRUNING prunes; SHADOW serves the full result but computes+diffs the specialized one.
_PRUNING_ENV = 'OSPREY_TYPED_CONTRACT_PRUNING'
_SHADOW_ENV = 'OSPREY_TYPED_CONTRACT_SHADOW'
_LAZY_SPECIALIZE_ENV = 'OSPREY_TYPED_CONTRACT_LAZY_SPECIALIZE'
_ALL = '*'
_FALSEY = {'', '0', 'false', 'no', 'off'}
_TRUTHY = {'1', 'true', 'yes', 'on', '*', 'all'}


def _parse_action_filter(env_name: str) -> FrozenSet[str]:
    """frozenset() when disabled, {'*'} for all (truthy scalar), else the named actions."""
    raw = os.environ.get(env_name, '').strip()
    if raw.lower() in _FALSEY:
        return frozenset()
    if raw.lower() in _TRUTHY:
        return frozenset({_ALL})
    return frozenset(p.strip() for p in raw.split(',') if p.strip())


def pruning_action_filter() -> FrozenSet[str]:
    return _parse_action_filter(_PRUNING_ENV)


def shadow_action_filter() -> FrozenSet[str]:
    return _parse_action_filter(_SHADOW_ENV)


def filter_includes(action_filter: FrozenSet[str], action_name: str) -> bool:
    return _ALL in action_filter or action_name in action_filter


def lazy_specialize_enabled() -> bool:
    """Whether the asyncio engine warms specializations in the background instead of
    specializing every allowlisted action inline on the reload path. Defaults ON.

    Set ``OSPREY_TYPED_CONTRACT_LAZY_SPECIALIZE=0`` to restore the inline-eager pass —
    the escape hatch if background warm-up ever misbehaves. Only the asyncio engine
    reads this; the gevent engine is always eager (see ``typed_contract_warmer``).
    """
    raw = os.environ.get(_LAZY_SPECIALIZE_ENV, '').strip().lower()
    if not raw:
        return True
    return raw not in _FALSEY


@dataclass(frozen=True)
class ActionSchema:
    """Parsed representation of a per-action schema JSON file."""

    action: str
    provides_groups: FrozenSet[str]  # keys of `provides`
    absent_groups: FrozenSet[str]  # from `absent`
    provides_field_types: Dict[str, str]  # "user.id" -> "int" (dot-notation, flattened)
    optional_for: Dict[str, List[str]]


class SchemaLoadError(Exception):
    """Raised when a schema file cannot be parsed or is invalid."""


def parse_schema(raw: Dict[str, Any], ref_reader: Callable[[str], Dict[str, Any]], where: str) -> ActionSchema:
    """Parse an already-decoded schema dict into an ActionSchema.

    This is the source-agnostic core shared by the disk loader (``load_schema``) and the
    in-memory loader (``load_schema_for_action_from_sources``). The two differ only in HOW
    they read raw JSON and resolve ``$ref:`` references.

    Args:
        raw: The decoded top-level schema JSON.
        ref_reader: Resolves a ``$ref:`` string (e.g. ``"$ref:types/user.json"``) to the
            decoded contents of the referenced type. Implementations must enforce any
            path-traversal guards and raise SchemaLoadError on escape / not-found.
        where: A human-readable location label (file path or sources key) for error messages.

    Raises:
        SchemaLoadError: if the schema is malformed or violates constraints.
    """
    version = raw.get('version')
    if version != _SUPPORTED_VERSION:
        raise SchemaLoadError(f'Unsupported schema version in {where}: {version!r}. Expected {_SUPPORTED_VERSION!r}.')

    action = raw.get('action', '')
    if not action:
        raise SchemaLoadError(f"Missing 'action' field in {where}")

    raw_provides: Dict[str, object] = raw.get('provides', {})
    absent_list: List[str] = raw.get('absent', [])
    types_used: Dict[str, str] = raw.get('types_used', {})
    optional_for: Dict[str, List[str]] = raw.get('optional_for', {})

    absent_groups: Set[str] = set(absent_list)
    provides_groups: Set[str] = set(raw_provides.keys())

    # Constraint: provides and absent must not overlap
    overlap = provides_groups & absent_groups
    if overlap:
        raise SchemaLoadError(f"Schema {where}: groups {overlap!r} appear in both 'provides' and 'absent'.")

    # Resolve $ref: references — load referenced type definitions and merge into provides.
    resolved_provides: Dict[str, object] = {}
    for group, value in raw_provides.items():
        if isinstance(value, str) and value.startswith('$ref:'):
            resolved_provides[group] = ref_reader(value)
        else:
            resolved_provides[group] = value

    # Also resolve top-level $ref entries in types_used if they provide field definitions
    for group, ref_str in types_used.items():
        if isinstance(ref_str, str) and ref_str.startswith('$ref:') and group not in resolved_provides:
            try:
                resolved_provides[group] = ref_reader(ref_str)
                provides_groups.add(group)
            except SchemaLoadError:
                # types_used refs are informational; skip if the type isn't available yet (a
                # missing/escaping ref here must not break loading the schema).
                pass

    # Flatten provides to dot-notation field types: "user.id" -> "int"
    # Scalar-only groups use the convention {"_scalar": "<type>"} to represent a
    # top-level field with no sub-fields (e.g. request_name: str rather than
    # request_name.field: str). Flatten these to the bare group name so that a
    # JsonData extraction of "$.request_name" matches "request_name" in
    # provides_field_types rather than the non-existent "request_name._scalar".
    provides_field_types: Dict[str, str] = {}
    for group, fields in resolved_provides.items():
        if isinstance(fields, dict):
            if list(fields.keys()) == ['_scalar'] and isinstance(fields['_scalar'], str):
                # Scalar group: flatten to bare group name
                provides_field_types[group] = fields['_scalar']
                continue
            for field_name, field_type in fields.items():
                if isinstance(field_type, str):
                    provides_field_types[f'{group}.{field_name}'] = field_type

    return ActionSchema(
        action=action,
        provides_groups=frozenset(provides_groups),
        absent_groups=frozenset(absent_groups),
        provides_field_types=provides_field_types,
        optional_for=optional_for,
    )


def load_schema(schema_path: Path, schemas_dir: Optional[Path] = None) -> ActionSchema:
    """Load and parse a single action schema JSON file from disk.

    Args:
        schema_path: Path to the <action_name>.json schema file.
        schemas_dir: Base directory for resolving $ref: paths. Defaults to
            the parent directory of schema_path.

    Returns:
        Parsed ActionSchema.

    Raises:
        SchemaLoadError: if the file is missing, malformed, or violates constraints.
    """
    if schemas_dir is None:
        schemas_dir = schema_path.parent

    try:
        raw = json.loads(schema_path.read_text())
    except FileNotFoundError:
        raise SchemaLoadError(f'Schema file not found: {schema_path}')
    except json.JSONDecodeError as e:
        raise SchemaLoadError(f'Invalid JSON in {schema_path}: {e}')

    # $ref values point to types/<name>.json relative to schemas_dir.
    _schemas_dir_resolved = schemas_dir.resolve()

    def _disk_ref_reader(ref_str: str) -> Dict[str, Any]:
        """Resolve a $ref: path on disk, asserting it stays within schemas_dir."""
        ref_rel = ref_str[len('$ref:') :]
        ref_path = (schemas_dir / ref_rel).resolve()
        if not ref_path.is_relative_to(_schemas_dir_resolved):
            raise SchemaLoadError(f'$ref path escapes schemas directory: {ref_rel!r} resolves to {ref_path}')
        try:
            return json.loads(ref_path.read_text())
        except FileNotFoundError:
            raise SchemaLoadError(f'Referenced type file not found: {ref_path} (from {schema_path})')
        except json.JSONDecodeError as e:
            raise SchemaLoadError(f'Invalid JSON in referenced file {ref_path}: {e}')

    return parse_schema(raw, _disk_ref_reader, where=str(schema_path))


def resolve_schemas_dir() -> Optional[Path]:
    """Discover the schemas directory from the runtime environment.

    Resolution order:
      1. ``OSPREY_SCHEMAS_DIR`` if set and points at an existing directory.
      2. ``<OSPREY_RULES_PATH>/schemas`` if that env var is set and the
         ``schemas`` subdirectory exists in the rules tree.

    Returns ``None`` if neither path resolves to an existing directory.

    The fallback to ``OSPREY_RULES_PATH/schemas`` lets typed contracts
    activate in environments that already point the worker at a smite-rules
    checkout, without requiring a separate env var or deployment change.
    """
    explicit = os.environ.get('OSPREY_SCHEMAS_DIR', '').strip()
    if explicit:
        candidate = Path(explicit)
        if candidate.is_dir():
            return candidate
        log.warning('OSPREY_SCHEMAS_DIR=%r is not a directory; ignoring', explicit)

    rules_path = os.environ.get('OSPREY_RULES_PATH', '').strip()
    if rules_path:
        candidate = Path(rules_path) / 'schemas'
        if candidate.is_dir():
            return candidate

    return None


def load_schema_for_action(action_name: str, schemas_dir: Path) -> Optional[ActionSchema]:
    """Load the schema for a given action name from the schemas directory.

    Returns None if no schema file exists for that action (schema-less actions
    fall back to the default execution graph).
    """
    schema_path = schemas_dir / f'{action_name}.json'
    if not schema_path.exists():
        return None
    try:
        return load_schema(schema_path, schemas_dir=schemas_dir)
    except SchemaLoadError:
        log.exception('Failed to load schema for action %r from %s', action_name, schema_path)
        return None


def load_schema_for_action_from_sources(action_name: str, schemas: Mapping[str, str]) -> Optional[ActionSchema]:
    """Load the schema for an action from the in-memory ``Sources`` schema map.

    ``schemas`` maps repo-relative posix paths (``schemas/<action>.json``,
    ``schemas/types/<name>.json``) to raw JSON contents — the same map that rides on the
    etcd Sources payload. This is the etcd-sourced counterpart to
    :func:`load_schema_for_action`: it lets #55's specializer activate on the prod worker,
    which has no schemas directory on disk.

    Returns None if no schema exists for ``action_name``.

    Raises:
        SchemaLoadError: if the schema (or a referenced type) is malformed, missing, or a
            ``$ref:`` attempts to escape the ``schemas/`` key space.
    """
    key = f'schemas/{action_name}.json'
    raw_text = schemas.get(key)
    if raw_text is None:
        return None

    def _sources_ref_reader(ref_str: str) -> Dict[str, Any]:
        """Resolve a $ref: against the schemas map, rejecting absolute / traversing paths."""
        ref_rel = ref_str[len('$ref:') :]
        # Reject absolute paths or any parent-traversal segment — refs must stay within the
        # `schemas/` key space, mirroring the disk loader's path-traversal guard.
        if ref_rel.startswith('/') or any(part == '..' for part in ref_rel.split('/')):
            raise SchemaLoadError(f'$ref path escapes schemas directory: {ref_rel!r}')
        ref_key = f'schemas/{ref_rel}'
        ref_text = schemas.get(ref_key)
        if ref_text is None:
            raise SchemaLoadError(f'Referenced type not found: {ref_key} (from {key})')
        try:
            return json.loads(ref_text)
        except json.JSONDecodeError as e:
            raise SchemaLoadError(f'Invalid JSON in referenced source {ref_key}: {e}')

    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise SchemaLoadError(f'Invalid JSON in {key}: {e}')
    return parse_schema(raw, _sources_ref_reader, where=key)
