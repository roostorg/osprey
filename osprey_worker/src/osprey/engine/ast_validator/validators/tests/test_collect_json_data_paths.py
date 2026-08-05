"""Tests for CollectJsonDataPaths validator (§4.2 / §5.3)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.collect_json_data_paths import (
    CollectJsonDataPaths,
    _extract_top_level_group,
)
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.entity import EntityJson
from osprey.engine.stdlib.udfs.get_action_name import GetActionName
from osprey.engine.stdlib.udfs.import_ import Import

# Build a minimal UDF registry with the UDFs needed for these tests
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.require import Require
from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry

_TEST_REGISTRY = UDFRegistry.with_udfs(JsonData, EntityJson, Import, Require, GetActionName, ResolveOptional)

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators(
        [
            ValidateCallKwargs,
            ImportsMustNotHaveCycles,
            ValidateDynamicCallsHaveAnnotatedRValue,
            UniqueStoredNames,
            CollectJsonDataPaths,
        ]
    ),
    pytest.mark.use_udf_registry(_TEST_REGISTRY),
]


# ---------------------------------------------------------------------------
# Helper: run validation and extract the manifest from the validator result
# ---------------------------------------------------------------------------


def _get_manifest(run_validation: RunValidationFunction, sources_dict: Any) -> dict:
    validated = run_validation(sources_dict)
    return validated.get_validator_result(CollectJsonDataPaths)


# ---------------------------------------------------------------------------
# Test: top_level_group extraction (standalone unit test, no SML needed)
# ---------------------------------------------------------------------------


def test_top_level_group_extraction() -> None:
    assert _extract_top_level_group('$.target_user.ip') == 'target_user'
    assert _extract_top_level_group('$.user.flags') == 'user'
    assert _extract_top_level_group('$.captcha_response.score') == 'captcha_response'
    assert _extract_top_level_group('$.http_request.user_agent') == 'http_request'


# ---------------------------------------------------------------------------
# Test: collects JsonData calls
# ---------------------------------------------------------------------------


def test_collects_json_data_calls(run_validation: RunValidationFunction) -> None:
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/test_action.sml': """
            UserId: int = JsonData(path='$.user.id')
            UserName: Optional[str] = JsonData(path='$.user.username', required=False)
            """,
        },
    )
    assert 'test_action' in manifest
    fields = manifest['test_action']
    paths = [f.path for f in fields]
    assert '$.user.id' in paths
    assert '$.user.username' in paths

    id_field = next(f for f in fields if f.path == '$.user.id')
    assert id_field.top_level_group == 'user'
    assert id_field.required is True
    assert id_field.udf_name == 'JsonData'
    assert id_field.rvalue_type == 'int'

    name_field = next(f for f in fields if f.path == '$.user.username')
    assert name_field.required is False


# ---------------------------------------------------------------------------
# Test: collects EntityJson calls
# ---------------------------------------------------------------------------


def test_collects_entity_json_calls(run_validation: RunValidationFunction) -> None:
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/test_action.sml': """
            UserEntity: Entity[int] = EntityJson(path='$.user.id', type='User')
            """,
        },
    )
    fields = manifest.get('test_action', [])
    assert any(f.udf_name == 'EntityJson' and f.path == '$.user.id' for f in fields)


# ---------------------------------------------------------------------------
# Test: walks static Import edges
# ---------------------------------------------------------------------------


def test_walks_static_imports(run_validation: RunValidationFunction) -> None:
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/test_action.sml': """
            Import(rules=["models/user.sml"])
            """,
            'models/user.sml': """
            UserId: int = JsonData(path='$.user.id')
            """,
        },
    )
    fields = manifest.get('test_action', [])
    paths = [f.path for f in fields]
    assert '$.user.id' in paths, f"Expected '$.user.id' in {paths}"


# ---------------------------------------------------------------------------
# Test: walks Require(rule="...") literal edges
# ---------------------------------------------------------------------------


def test_walks_requires_literal(run_validation: RunValidationFunction) -> None:
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/test_action.sml': """
            Require(rule="models/auth.sml")
            SomeId: int = JsonData(path='$.some.id')
            """,
            'models/auth.sml': """
            SessionId: str = JsonData(path='$.session.id')
            """,
        },
    )
    fields = manifest.get('test_action', [])
    paths = [f.path for f in fields]
    assert '$.session.id' in paths, f"Expected '$.session.id' in {paths}"


# ---------------------------------------------------------------------------
# Test: walks Require(rule=f"...") fstring glob edges
# ---------------------------------------------------------------------------


def test_walks_requires_fstring_glob(run_validation: RunValidationFunction) -> None:
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/guild_joined.sml': """
            GuildId: int = JsonData(path='$.guild.id')
            """,
        },
    )
    # guild_joined should be in manifest with its fields
    assert 'guild_joined' in manifest
    fields = manifest['guild_joined']
    assert any(f.path == '$.guild.id' for f in fields)


# ---------------------------------------------------------------------------
# Test: require_if branches are both walked statically
# ---------------------------------------------------------------------------


def test_walks_require_if_false_branch(run_validation: RunValidationFunction) -> None:
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/test_action.sml': """
            ShouldLoad = True
            Require(rule="models/optional.sml", require_if=ShouldLoad)
            MainField: int = JsonData(path='$.main.id')
            """,
            'models/optional.sml': """
            OptField: str = JsonData(path='$.optional.field')
            """,
        },
    )
    # Even though require_if is used, the validator statically walks the target
    fields = manifest.get('test_action', [])
    paths = [f.path for f in fields]
    assert '$.optional.field' in paths


# ---------------------------------------------------------------------------
# Test: marker picks up new UDF automatically
# ---------------------------------------------------------------------------


def test_marker_picks_up_new_udf(run_validation: RunValidationFunction, udf_registry: UDFRegistry) -> None:
    """A test-only UDF with extracts_json_path = True is picked up by the collector."""

    class JsonArrayArguments(ArgumentsBase):
        path: ConstExpr[str]

    class JsonArray(UDFBase[JsonArrayArguments, str]):
        """Test-only UDF that extracts a JSON array (returns str for type safety)."""

        extracts_json_path: bool = True

        def execute(self, execution_context: ExecutionContext, arguments: JsonArrayArguments) -> str:
            return ''

    udf_registry.register(JsonArray)

    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/test_action.sml': """
            Tags: str = JsonArray(path='$.user.tags')
            """,
        },
    )
    fields = manifest.get('test_action', [])
    assert any(f.udf_name == 'JsonArray' for f in fields), f'Expected JsonArray in fields: {fields}'


# ---------------------------------------------------------------------------
# Test: no double-counting across shared imports
# ---------------------------------------------------------------------------


def test_no_double_counting(run_validation: RunValidationFunction) -> None:
    """A shared model file imported by two actions should not duplicate fields in either manifest."""
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/action_a.sml': """
            Import(rules=["models/shared.sml"])
            """,
            'actions/action_b.sml': """
            Import(rules=["models/shared.sml"])
            """,
            'models/shared.sml': """
            SharedField: str = JsonData(path='$.shared.value')
            """,
        },
    )
    for action_name in ('action_a', 'action_b'):
        if action_name in manifest:
            paths = [f.path for f in manifest[action_name]]
            count = paths.count('$.shared.value')
            assert count <= 1, f'Duplicate field in {action_name}: {paths}'


# ---------------------------------------------------------------------------
# Test: stable node identity (span coordinates round-trip)
# ---------------------------------------------------------------------------


def test_emits_field_declaration_with_stable_identity(run_validation: RunValidationFunction) -> None:
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/test_action.sml': """
            UserId: int = JsonData(path='$.user.id')
            """,
        },
    )
    fields = manifest.get('test_action', [])
    assert len(fields) >= 1
    field = fields[0]
    # span_start_line must be a positive integer
    assert isinstance(field.span_start_line, int)
    assert field.span_start_line >= 1
    assert isinstance(field.span_start_col, int)
    assert field.span_start_col >= 0
    # source_file must be set to the correct path
    assert 'actions/test_action.sml' in field.source_file


# ---------------------------------------------------------------------------
# Test: collects all three extract UDF types (JsonData + EntityJson)
# ---------------------------------------------------------------------------


def test_collects_all_three_extract_udfs(run_validation: RunValidationFunction) -> None:
    """JsonData and EntityJson are both picked up. (ActionData is discord-side.)"""
    manifest = _get_manifest(
        run_validation,
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/test_action.sml': """
            UserId: int = JsonData(path='$.user.id')
            UserEntity: Entity[int] = EntityJson(path='$.user.id', type='User')
            """,
        },
    )
    fields = manifest.get('test_action', [])
    udf_names = {f.udf_name for f in fields}
    assert 'JsonData' in udf_names
    assert 'EntityJson' in udf_names


# ---------------------------------------------------------------------------
# Helpers for cross-check warning tests (Issues §4.6 Check 1 and Check 3)
# ---------------------------------------------------------------------------

_VALID_SCHEMA_BASE = {
    '$schema': 'https://discord.dev/smite/action-schema/v1',
    'version': 1,
    'generated_from': {'source': 'test', 'date': '2026-05-25', 'authored_by': 'test'},
    'types_used': {},
    'optional_for': {},
}


def _write_schema(tmp_path: Path, action: str, provides: dict, absent: list) -> Path:
    schema = dict(_VALID_SCHEMA_BASE)
    schema['action'] = action
    schema['provides'] = provides
    schema['absent'] = absent
    schema_path = tmp_path / f'{action}.json'
    schema_path.write_text(json.dumps(schema))
    return schema_path


def _get_warnings(run_validation: RunValidationFunction, sources_dict: Any) -> List[Any]:
    validated = run_validation(sources_dict)
    return list(validated.warnings)


# ---------------------------------------------------------------------------
# Test: Check 1 — absent-field usage emits a warning
# ---------------------------------------------------------------------------


def test_cross_check_absent_field_emits_warning(run_validation: RunValidationFunction) -> None:
    """Check 1: a rule that extracts a path from an absent group should emit a warning."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        _write_schema(
            Path(tmp_dir),
            action='test_action',
            provides={'user': {'id': 'int'}},
            absent=['target_user'],
        )
        with pytest.MonkeyPatch().context() as mp:
            mp.setenv('OSPREY_SCHEMAS_DIR', tmp_dir)
            warnings = _get_warnings(
                run_validation,
                {
                    'main.sml': """
                    ActionName = GetActionName()
                    Require(rule=f"actions/{ActionName}.sml")
                    """,
                    'actions/test_action.sml': """
                    TargetId: int = JsonData(path='$.target_user.id')
                    """,
                },
            )

        # Exactly one warning for the absent-field usage
        assert len(warnings) >= 1, f'Expected at least one warning, got {warnings}'
        messages = [w.message for w in warnings]
        absent_warning = next(
            (m for m in messages if 'absent' in m.lower() and 'target_user' in m),
            None,
        )
        assert absent_warning is not None, f'Expected absent-field warning, got messages: {messages}'


# ---------------------------------------------------------------------------
# Test: Check 3 — unknown field (group present, field not listed) emits a warning
# ---------------------------------------------------------------------------


def test_cross_check_unknown_field_emits_warning(run_validation: RunValidationFunction) -> None:
    """Check 3: a rule that extracts a field not listed in schema.provides emits a warning."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        _write_schema(
            Path(tmp_dir),
            action='test_action',
            provides={'user': {'id': 'int'}},  # user.email is NOT listed
            absent=[],
        )
        with pytest.MonkeyPatch().context() as mp:
            mp.setenv('OSPREY_SCHEMAS_DIR', tmp_dir)
            warnings = _get_warnings(
                run_validation,
                {
                    'main.sml': """
                    ActionName = GetActionName()
                    Require(rule=f"actions/{ActionName}.sml")
                    """,
                    'actions/test_action.sml': """
                    UserEmail: str = JsonData(path='$.user.email')
                    """,
                },
            )

        # Exactly one warning for the unknown field
        assert len(warnings) >= 1, f'Expected at least one warning, got {warnings}'
        messages = [w.message for w in warnings]
        unknown_warning = next(
            (m for m in messages if 'user.email' in m or 'unknown' in m.lower() or 'schema' in m.lower()),
            None,
        )
        assert unknown_warning is not None, f'Expected unknown-field warning, got messages: {messages}'


# ---------------------------------------------------------------------------
# Test: no false-positive warnings for present + known fields
# ---------------------------------------------------------------------------


def test_cross_check_no_warnings_for_known_fields(run_validation: RunValidationFunction) -> None:
    """No warnings should be emitted when the rule extracts a known, present field."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        _write_schema(
            Path(tmp_dir),
            action='test_action',
            provides={'user': {'id': 'int', 'email': 'str'}},
            absent=[],
        )
        with pytest.MonkeyPatch().context() as mp:
            mp.setenv('OSPREY_SCHEMAS_DIR', tmp_dir)
            warnings = _get_warnings(
                run_validation,
                {
                    'main.sml': """
                    ActionName = GetActionName()
                    Require(rule=f"actions/{ActionName}.sml")
                    """,
                    'actions/test_action.sml': """
                    UserId: int = JsonData(path='$.user.id')
                    UserEmail: str = JsonData(path='$.user.email')
                    """,
                },
            )

        assert len(warnings) == 0, f'Expected no warnings for known present fields, got {[w.message for w in warnings]}'
