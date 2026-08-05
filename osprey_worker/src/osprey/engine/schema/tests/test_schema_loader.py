"""Tests for the ActionSchema loader (§4.3 / §5.3)."""

import json
from pathlib import Path

import pytest
from osprey.engine.schema.schema_loader import (
    SchemaLoadError,
    load_schema,
    load_schema_for_action,
    load_schema_for_action_from_sources,
    resolve_schemas_dir,
)

_VALID_SCHEMA = {
    'action': 'guild_joined',
    'version': 1,
    'generated_from': {
        'source': 'hand-authored',
        'date': '2026-05-25',
        'authored_by': 'ls/typed-action-contracts',
    },
    'provides': {
        'user': {'id': 'int', 'username': 'str'},
        'guild': {'id': 'int', 'name': 'str', 'member_count': 'int'},
    },
    'absent': ['target_user', 'captcha_response', 'oauth2_request_data'],
    'types_used': {},
    'optional_for': {'captcha_response': ['password_login']},
}


def _write_schema(tmp_path: Path, data: dict, name: str = 'guild_joined.json') -> Path:
    schema_path = tmp_path / name
    schema_path.write_text(json.dumps(data))
    return schema_path


class TestSchemaLoader:
    def test_parse_valid_schema(self, tmp_path: Path) -> None:
        path = _write_schema(tmp_path, _VALID_SCHEMA)
        schema = load_schema(path)

        assert schema.action == 'guild_joined'
        assert 'user' in schema.provides_groups
        assert 'guild' in schema.provides_groups
        assert 'target_user' in schema.absent_groups
        assert 'captcha_response' in schema.absent_groups
        assert 'oauth2_request_data' in schema.absent_groups
        assert schema.provides_field_types['user.id'] == 'int'
        assert schema.provides_field_types['user.username'] == 'str'
        assert schema.provides_field_types['guild.member_count'] == 'int'
        assert schema.optional_for['captcha_response'] == ['password_login']

    def test_schema_provides_absent_mutually_exclusive(self, tmp_path: Path) -> None:
        bad = dict(_VALID_SCHEMA)
        bad['absent'] = ['user', 'target_user']  # "user" is also in provides
        path = _write_schema(tmp_path, bad)
        with pytest.raises(SchemaLoadError, match="both 'provides' and 'absent'"):
            load_schema(path)

    def test_missing_schema_version_raises(self, tmp_path: Path) -> None:
        bad = dict(_VALID_SCHEMA)
        bad['version'] = 99
        path = _write_schema(tmp_path, bad)
        with pytest.raises(SchemaLoadError, match='Unsupported schema version'):
            load_schema(path)

    def test_missing_action_field_raises(self, tmp_path: Path) -> None:
        bad = {k: v for k, v in _VALID_SCHEMA.items() if k != 'action'}
        path = _write_schema(tmp_path, bad)
        with pytest.raises(SchemaLoadError, match="Missing 'action'"):
            load_schema(path)

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(SchemaLoadError, match='not found'):
            load_schema(tmp_path / 'nonexistent.json')

    def test_invalid_json_raises(self, tmp_path: Path) -> None:
        bad_path = tmp_path / 'bad.json'
        bad_path.write_text('{ not valid json }')
        with pytest.raises(SchemaLoadError, match='Invalid JSON'):
            load_schema(bad_path)

    def test_ref_resolution(self, tmp_path: Path) -> None:
        # Create a type reference file
        types_dir = tmp_path / 'types'
        types_dir.mkdir()
        (types_dir / 'user.json').write_text(json.dumps({'id': 'int', 'username': 'str'}))

        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:types/user.json', 'guild': {'id': 'int'}}
        path = _write_schema(tmp_path, schema_data)

        schema = load_schema(path, schemas_dir=tmp_path)
        assert schema.provides_field_types['user.id'] == 'int'
        assert schema.provides_field_types['user.username'] == 'str'
        assert schema.provides_field_types['guild.id'] == 'int'

    def test_ref_file_missing_raises(self, tmp_path: Path) -> None:
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:types/nonexistent.json'}
        path = _write_schema(tmp_path, schema_data)
        with pytest.raises(SchemaLoadError, match='not found'):
            load_schema(path, schemas_dir=tmp_path)

    def test_load_schema_for_action_returns_none_if_missing(self, tmp_path: Path) -> None:
        result = load_schema_for_action('nonexistent_action', tmp_path)
        assert result is None

    def test_load_schema_for_action_finds_by_name(self, tmp_path: Path) -> None:
        _write_schema(tmp_path, _VALID_SCHEMA, 'guild_joined.json')
        schema = load_schema_for_action('guild_joined', tmp_path)
        assert schema is not None
        assert schema.action == 'guild_joined'

    def test_absent_groups_are_frozenset(self, tmp_path: Path) -> None:
        path = _write_schema(tmp_path, _VALID_SCHEMA)
        schema = load_schema(path)
        # Verify immutability
        assert isinstance(schema.absent_groups, frozenset)
        assert isinstance(schema.provides_groups, frozenset)

    def test_ref_path_traversal_relative_escapes(self, tmp_path: Path) -> None:
        """$ref with relative traversal (../../etc/passwd) must raise SchemaLoadError."""
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:../../etc/passwd'}
        path = _write_schema(tmp_path, schema_data)
        with pytest.raises(SchemaLoadError, match='escapes schemas directory'):
            load_schema(path, schemas_dir=tmp_path)

    def test_ref_path_traversal_absolute_injection(self, tmp_path: Path) -> None:
        """$ref with an absolute path should not escape schemas_dir."""
        # An absolute path injected via $ref: /tmp/secret would resolve outside schemas_dir
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:/etc/hostname'}
        path = _write_schema(tmp_path, schema_data)
        with pytest.raises(SchemaLoadError, match='escapes schemas directory'):
            load_schema(path, schemas_dir=tmp_path)

    def test_scalar_group_flattens_to_bare_key(self, tmp_path: Path) -> None:
        """A provides entry {"_scalar": "<type>"} must flatten to the bare group name."""
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {
            'user': {'id': 'int', 'username': 'str'},
            'request_name': {'_scalar': 'str'},
        }
        path = _write_schema(tmp_path, schema_data)
        schema = load_schema(path)

        # Scalar group: bare key, not dotted
        assert schema.provides_field_types['request_name'] == 'str'
        # No dotted variant must exist
        assert 'request_name._scalar' not in schema.provides_field_types
        # Regular group still works
        assert schema.provides_field_types['user.id'] == 'int'
        # Group name still appears in provides_groups
        assert 'request_name' in schema.provides_groups

    def test_scalar_group_in_cross_check_does_not_produce_false_unknown_field(self, tmp_path: Path) -> None:
        """A path_key of 'request_name' must match provides_field_types['request_name'].

        This is a unit-level regression guard: after _scalar flattening, Check 3 in
        CollectJsonDataPaths must NOT fire a false "unknown field" warning for scalar
        groups.  The lookup path_key = path.removeprefix('$.') == 'request_name' must
        find provides_field_types['request_name'], not 'request_name._scalar'.
        """
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {
            'user': {'id': 'int'},
            'request_name': {'_scalar': 'str'},
        }
        path = _write_schema(tmp_path, schema_data)
        schema = load_schema(path)

        # Simulate what _cross_check_types does: path_key = "$.request_name".removeprefix("$.")
        path_key = '$.request_name'.removeprefix('$.')
        declared = schema.provides_field_types.get(path_key)
        assert declared == 'str', (
            f'Check 2 lookup for path_key={path_key!r} returned {declared!r}; '
            "expected 'str'. Without _scalar flattening this returns None, which "
            "would cause Check 3 to fire a false 'unknown field' warning."
        )

    def test_schema_with_no_provides_and_no_absent(self, tmp_path: Path) -> None:
        minimal = {
            'action': 'minimal_action',
            'version': 1,
            'generated_from': {'source': 'hand-authored', 'date': '2026-05-25', 'authored_by': 'test'},
            'provides': {},
            'absent': [],
            'types_used': {},
            'optional_for': {},
        }
        path = _write_schema(tmp_path, minimal, 'minimal_action.json')
        schema = load_schema(path)
        assert schema.action == 'minimal_action'
        assert len(schema.provides_groups) == 0
        assert len(schema.absent_groups) == 0
        assert len(schema.provides_field_types) == 0


class TestLoadSchemaFromSources:
    """``load_schema_for_action_from_sources`` reads schemas from the in-memory
    ``Sources._schemas`` map (etcd payload) rather than disk — this is what activates typed
    contracts on the etcd-sourced prod worker.
    """

    def test_finds_by_name(self) -> None:
        schemas = {'schemas/guild_joined.json': json.dumps(_VALID_SCHEMA)}
        schema = load_schema_for_action_from_sources('guild_joined', schemas)
        assert schema is not None
        assert schema.action == 'guild_joined'
        assert 'user' in schema.provides_groups

    def test_returns_none_if_missing(self) -> None:
        schemas = {'schemas/guild_joined.json': json.dumps(_VALID_SCHEMA)}
        assert load_schema_for_action_from_sources('other_action', schemas) is None
        assert load_schema_for_action_from_sources('guild_joined', {}) is None

    def test_resolves_ref_from_map(self) -> None:
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:types/user.json', 'guild': {'id': 'int'}}
        schemas = {
            'schemas/guild_joined.json': json.dumps(schema_data),
            'schemas/types/user.json': json.dumps({'id': 'int', 'username': 'str'}),
        }
        schema = load_schema_for_action_from_sources('guild_joined', schemas)
        assert schema is not None
        assert schema.provides_field_types['user.id'] == 'int'
        assert schema.provides_field_types['user.username'] == 'str'
        assert schema.provides_field_types['guild.id'] == 'int'

    def test_missing_ref_raises(self) -> None:
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:types/nonexistent.json'}
        schemas = {'schemas/guild_joined.json': json.dumps(schema_data)}
        with pytest.raises(SchemaLoadError, match='not found'):
            load_schema_for_action_from_sources('guild_joined', schemas)

    def test_relative_traversal_ref_raises(self) -> None:
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:../../etc/passwd'}
        schemas = {'schemas/guild_joined.json': json.dumps(schema_data)}
        with pytest.raises(SchemaLoadError, match='escapes schemas directory'):
            load_schema_for_action_from_sources('guild_joined', schemas)

    def test_absolute_ref_raises(self) -> None:
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:/etc/hostname'}
        schemas = {'schemas/guild_joined.json': json.dumps(schema_data)}
        with pytest.raises(SchemaLoadError, match='escapes schemas directory'):
            load_schema_for_action_from_sources('guild_joined', schemas)

    def test_parity_disk_vs_from_sources(self, tmp_path: Path) -> None:
        """The same JSON loaded via disk ``load_schema`` and via the from-sources path must
        yield equal ActionSchema objects (incl. a resolved $ref)."""
        schema_data = dict(_VALID_SCHEMA)
        schema_data['provides'] = {'user': '$ref:types/user.json', 'guild': {'id': 'int'}}
        type_data = {'id': 'int', 'username': 'str'}

        # Disk
        types_dir = tmp_path / 'types'
        types_dir.mkdir()
        (types_dir / 'user.json').write_text(json.dumps(type_data))
        disk_path = _write_schema(tmp_path, schema_data)
        disk_schema = load_schema(disk_path, schemas_dir=tmp_path)

        # From sources
        schemas = {
            'schemas/guild_joined.json': json.dumps(schema_data),
            'schemas/types/user.json': json.dumps(type_data),
        }
        sources_schema = load_schema_for_action_from_sources('guild_joined', schemas)

        assert sources_schema == disk_schema


class TestResolveSchemasDir:
    """resolve_schemas_dir picks up schemas without an explicit env var when
    OSPREY_RULES_PATH already points at a smite-rules checkout that has a
    ``schemas/`` subdirectory — this is what makes typed contracts activate in
    prod without requiring a separate deployment change.
    """

    def test_returns_none_when_no_env_set(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.delenv('OSPREY_SCHEMAS_DIR', raising=False)
        monkeypatch.delenv('OSPREY_RULES_PATH', raising=False)
        assert resolve_schemas_dir() is None

    def test_explicit_schemas_dir_wins(self, monkeypatch, tmp_path: Path) -> None:
        schemas = tmp_path / 'explicit'
        schemas.mkdir()
        rules = tmp_path / 'rules'
        (rules / 'schemas').mkdir(parents=True)
        monkeypatch.setenv('OSPREY_SCHEMAS_DIR', str(schemas))
        monkeypatch.setenv('OSPREY_RULES_PATH', str(rules))
        assert resolve_schemas_dir() == schemas

    def test_falls_back_to_rules_path_schemas_subdir(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.delenv('OSPREY_SCHEMAS_DIR', raising=False)
        rules = tmp_path / 'smite-rules'
        (rules / 'schemas').mkdir(parents=True)
        monkeypatch.setenv('OSPREY_RULES_PATH', str(rules))
        assert resolve_schemas_dir() == rules / 'schemas'

    def test_returns_none_when_rules_path_has_no_schemas_subdir(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.delenv('OSPREY_SCHEMAS_DIR', raising=False)
        rules = tmp_path / 'smite-rules'
        rules.mkdir()  # no schemas/ subdir
        monkeypatch.setenv('OSPREY_RULES_PATH', str(rules))
        assert resolve_schemas_dir() is None

    def test_ignores_explicit_dir_that_does_not_exist(self, monkeypatch, tmp_path: Path) -> None:
        rules = tmp_path / 'smite-rules'
        (rules / 'schemas').mkdir(parents=True)
        monkeypatch.setenv('OSPREY_SCHEMAS_DIR', str(tmp_path / 'does-not-exist'))
        monkeypatch.setenv('OSPREY_RULES_PATH', str(rules))
        # Explicit dir is invalid → falls through to OSPREY_RULES_PATH/schemas
        assert resolve_schemas_dir() == rules / 'schemas'
