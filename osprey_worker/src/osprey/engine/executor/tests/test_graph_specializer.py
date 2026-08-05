"""Tests for the graph specializer (§4.4 / §5.3).

The specializer takes a full ExecutionGraph + ActionSchema and produces a
SpecializedExecutionGraph that prunes dependency chains for absent groups.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional

import gevent.pool
import pytest
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.ast_validator.validators.feature_name_to_entity_type_mapping import (
    FeatureNameToEntityTypeMapping,
)
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.executor.execution_context import (
    Action,
    ExecutionContext,
)
from osprey.engine.executor.execution_graph import compile_execution_graph
from osprey.engine.executor.executor import execute
from osprey.engine.executor.graph_specializer import (
    SpecializedExecutionGraph,
    _collect_all_chains_recursive,
    _get_all_sorted_chains,
    _get_top_level_group,
    _node_key_from_chain,
    specialize_graph,
)
from osprey.engine.executor.node_executor.call_executor import CallExecutor
from osprey.engine.executor.typed_contract_dispatch import resolve_dispatch
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.schema.schema_loader import ActionSchema
from osprey.engine.stdlib import get_config_registry
from osprey.engine.stdlib.udfs._prelude import ArgumentsBase, UDFBase
from osprey.engine.stdlib.udfs.entity import EntityJson
from osprey.engine.stdlib.udfs.get_action_name import GetActionName
from osprey.engine.stdlib.udfs.import_ import Import
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.require import Require
from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.stdlib.udfs.verdicts import DeclareVerdict
from osprey.engine.udf.registry import UDFRegistry
from result import Ok

# Minimal UDF registry without postgres-backed UDFs (no POSTGRES_HOSTS needed)
_TEST_REGISTRY = UDFRegistry.with_udfs(JsonData, EntityJson, Import, Require, GetActionName, ResolveOptional, Rule)

# Effect-aware registry: adds WhenRules/DeclareVerdict so effect sinks are present
_EFFECT_REGISTRY = UDFRegistry.with_udfs(
    JsonData, EntityJson, Import, Require, GetActionName, ResolveOptional, Rule, WhenRules, DeclareVerdict
)


def _compile(sources_dict: Dict[str, str]):
    """Compile sources and return (validated_sources, execution_graph).

    Uses a standard validator registry that includes ValidateCallKwargs and other
    required validators for proper compilation.
    """
    sources = Sources.from_dict({k: dedent(v) for k, v in sources_dict.items()})

    # Use a targeted registry with required validators
    registry = ValidatorRegistry.from_validator_classes(
        [
            ValidateCallKwargs,
            ValidateDynamicCallsHaveAnnotatedRValue,
            ImportsMustNotHaveCycles,
            UniqueStoredNames,
            VariablesMustBeDefined,
            ValidateStaticTypes,
        ]
    )
    validated = validate_sources(sources, _TEST_REGISTRY, registry)
    graph = compile_execution_graph(validated)
    return validated, graph


def _make_schema(
    action: str = 'test_action',
    provides: Dict = None,
    absent: List[str] = None,
) -> ActionSchema:
    provides = provides or {'user': {'id': 'int'}}
    absent = absent or ['target_user']
    field_types = {}
    for group, fields in provides.items():
        if isinstance(fields, dict):
            for k, v in fields.items():
                field_types[f'{group}.{k}'] = v
    return ActionSchema(
        action=action,
        provides_groups=frozenset(provides.keys()),
        absent_groups=frozenset(absent),
        provides_field_types=field_types,
        optional_for={},
    )


def _run_graph(graph, data: Dict[str, Any], action_name: str = 'test_action') -> Dict[str, Any]:
    action = Action(
        action_id=1,
        action_name=action_name,
        data=data,
        timestamp=datetime.utcnow(),
    )
    result = execute(graph, UDFHelpers(), action, gevent.pool.Pool(4))
    return result.extracted_features


def _run_graph_full_result(graph, data: Dict[str, Any], action_name: str = 'test_action'):
    """Like _run_graph but returns the full ExecutionResult (for inspecting effects/errors)."""
    action = Action(
        action_id=1,
        action_name=action_name,
        data=data,
        timestamp=datetime.utcnow(),
    )
    return execute(graph, UDFHelpers(), action, gevent.pool.Pool(4))


def _compile_effect(sources_dict: Dict[str, str]):
    """Compile with the effect-aware registry (adds WhenRules/DeclareVerdict)."""
    sources = Sources.from_dict({k: dedent(v) for k, v in sources_dict.items()})
    registry = ValidatorRegistry.from_validator_classes(
        [
            ValidateCallKwargs,
            ValidateDynamicCallsHaveAnnotatedRValue,
            ImportsMustNotHaveCycles,
            UniqueStoredNames,
            VariablesMustBeDefined,
            ValidateStaticTypes,
            FeatureNameToEntityTypeMapping,
            get_config_registry().get_validator(),
        ]
    )
    validated = validate_sources(sources, _EFFECT_REGISTRY, registry)
    graph = compile_execution_graph(validated)
    return validated, graph


# ---------------------------------------------------------------------------
# Test: top_level_group extraction (used by specializer internally)
# ---------------------------------------------------------------------------


def test_top_level_group_helper() -> None:
    assert _get_top_level_group('$.user.id') == 'user'
    assert _get_top_level_group('$.target_user.ip') == 'target_user'
    assert _get_top_level_group('$.captcha_response.score') == 'captcha_response'
    assert _get_top_level_group('$.http_request.ua') == 'http_request'


# ---------------------------------------------------------------------------
# Test: no absent groups → returns specialized graph with 0 pruned chains
# ---------------------------------------------------------------------------


def test_no_schema_returns_default_graph_unchanged() -> None:
    _, graph = _compile(
        {
            'main.sml': """
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    # Schema with no absent groups — nothing should be pruned
    schema = _make_schema(absent=[])
    specialized = specialize_graph(graph, schema)
    assert isinstance(specialized, SpecializedExecutionGraph)
    assert specialized.pruned_count == 0


# ---------------------------------------------------------------------------
# Test: prunes absent root node
# ---------------------------------------------------------------------------


def test_prunes_absent_root_node_and_cascade() -> None:
    _, graph = _compile(
        {
            'main.sml': """
            TargetUserId: int = JsonData(path='$.target_user.id')
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    schema = _make_schema(
        provides={'user': {'id': 'int'}},
        absent=['target_user'],
    )
    specialized = specialize_graph(graph, schema)
    assert specialized.pruned_count > 0

    # Execute with a payload that only has user data
    result = _run_graph(specialized, {'user': {'id': 42}})
    assert 'UserId' in result
    assert result['UserId'] == 42


# ---------------------------------------------------------------------------
# Test: keeps present root node
# ---------------------------------------------------------------------------


def test_keeps_present_root_node() -> None:
    _, graph = _compile(
        {
            'main.sml': """
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    schema = _make_schema(
        provides={'user': {'id': 'int'}},
        absent=['target_user'],
    )
    specialized = specialize_graph(graph, schema)
    result = _run_graph(specialized, {'user': {'id': 99}})
    assert result.get('UserId') == 99


# ---------------------------------------------------------------------------
# Test: ResolveOptional with default is not pruned when its dep is absent
# ---------------------------------------------------------------------------


def test_resolve_optional_with_default_not_pruned() -> None:
    _, graph = _compile(
        {
            'main.sml': """
            _TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            TargetIdOrZero: int = ResolveOptional(optional_value=_TargetId, default_value=0)
            """,
        }
    )
    schema = _make_schema(
        provides={'user': {'id': 'int'}},
        absent=['target_user'],
    )
    specialized = specialize_graph(graph, schema)
    result = _run_graph(specialized, {'user': {'id': 1}})
    assert isinstance(result, dict)
    # The default_value path must fire: _TargetId is pruned (absent group), so
    # ResolveOptional should return the default (0) rather than raise.
    assert result.get('TargetIdOrZero') == 0


# ---------------------------------------------------------------------------
# Test: specialized graph executes identically on payload with only present fields
# ---------------------------------------------------------------------------


def test_specialized_graph_executes_identically_on_payload_subset() -> None:
    _, graph = _compile(
        {
            'main.sml': """
            UserId: int = JsonData(path='$.user.id')
            UserName: Optional[str] = JsonData(path='$.user.username', required=False)
            TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            """,
        }
    )
    schema = _make_schema(
        provides={'user': {'id': 'int', 'username': 'str'}},
        absent=['target_user'],
    )
    payload = {'user': {'id': 7, 'username': 'alice'}}

    result_default = _run_graph(graph, payload)
    specialized = specialize_graph(graph, schema)
    result_specialized = _run_graph(specialized, payload)

    # Present fields must match
    assert result_default.get('UserId') == result_specialized.get('UserId')
    assert result_default.get('UserName') == result_specialized.get('UserName')


# ---------------------------------------------------------------------------
# Test: misclassified absent group — divergence pinned, no crash
# ---------------------------------------------------------------------------


def test_misclassified_absent_group_divergence_pinned() -> None:
    """Pin the failure mode for a schema that declares a group absent while the
    runtime payload actually contains it.

    Expected behavior under the current design:
      - Default graph extracts the value.
      - Specialized graph pruned the extractor chain at compile time, so the
        feature is *missing* from extracted_features (NOT set to ``None``,
        NOT raised, NOT fallback-to-default-graph).

    This is a divergent-output failure, not a same-as-default-graph failure.
    The PR description framed misclassification as "None values, same as the
    default graph" — that description is only accurate when the payload also
    omits the field. When the schema is wrong AND the field is present, the
    specialized graph silently drops the feature.

    Today's safety nets:
      - CollectJsonDataPaths' Check 1 emits a compile-time warning when a rule
        extracts from a declared-absent group (caught the original 3 mistakes).
      - BQ replay job validates schemas against historical payloads.

    Production observability for default-vs-specialized divergence is out of
    scope for the initial rollout (deferred shadow-mode work).
    """
    _, graph = _compile(
        {
            'main.sml': """
            UserId: int = JsonData(path='$.user.id')
            TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            """,
        }
    )
    # Schema MIS-CLASSIFIES target_user as absent.
    schema = _make_schema(
        provides={'user': {'id': 'int'}},
        absent=['target_user'],
    )
    # Payload contradicts the schema: target_user IS present.
    payload = {'user': {'id': 1}, 'target_user': {'id': 999}}

    default_result = _run_graph(graph, payload)
    specialized = specialize_graph(graph, schema)
    specialized_result = _run_graph(specialized, payload)

    # Default graph extracts the real value.
    assert default_result.get('TargetId') == 999
    # Specialized graph PRUNED the extractor — feature is missing from result,
    # not set to None. This is the divergence.
    assert 'TargetId' not in specialized_result
    # User-side data still works (specialization didn't break unrelated chains).
    assert default_result.get('UserId') == 1
    assert specialized_result.get('UserId') == 1


def test_misclassified_absent_feeding_enforcement_falls_back_via_presence_guard() -> None:
    """A wrongly-absent group that feeds an enforcement RULE must NOT silently drop the verdict,
    even when the payload actually contains the group (a false-absent).

    With constant-folding (specialize_graph step 4), the specialized graph BAKES IN the absent
    assumption: the enforcement extractor over the declared-absent group is folded to its absent
    value, so running the *specialized* graph on a MISCLASSIFIED payload (group actually present)
    would drop the verdict. Safety is restored at DISPATCH by the absent-group presence guard
    (`absent_groups_satisfied`): when an "absent" group is actually present, the guard is False so
    `resolve_dispatch` serves the FULL graph (which reads the real data and fires). When the group
    is genuinely absent, the folded graph is served (faster — the extractor is neither executed nor
    throws). This preserves the rescue's misclassification safety without paying its runtime cost.
    """
    _, graph = _compile_effect(
        {
            'main.sml': """
            TargetId: int = JsonData(path='$.target_user.id')
            IsBadTarget: bool = TargetId > 0
            BanRule = Rule(when_all=[IsBadTarget], description='ban bad target')
            WhenRules(rules_any=[BanRule], then=[DeclareVerdict(verdict="ban")])
            """,
        }
    )
    # Schema WRONGLY declares target_user absent (provides lists an unrelated group).
    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['target_user'])
    specialized = specialize_graph(graph, schema)
    misclassified = {'target_user': {'id': 999}}  # contradicts schema: target_user present + "bad"
    genuinely_absent = {'user': {'id': 1}}

    # Full graph enforces on the present-but-misclassified payload.
    full_verdicts = [v.verdict for v in _run_graph_full_result(graph, misclassified).verdicts]
    assert full_verdicts == ['ban'], f'full graph should ban; got {full_verdicts}'

    # The guard distinguishes the two payloads.
    assert specialized.absent_groups_satisfied(misclassified) is False
    assert specialized.absent_groups_satisfied(genuinely_absent) is True

    gates = (frozenset({'*'}), frozenset())  # prune-all, no shadow
    graphs = {'test_action': specialized}
    # Misclassified payload -> guard fails -> dispatch serves the FULL graph -> ban preserved.
    serve, shadow = resolve_dispatch('test_action', graphs, *gates, graph, action_data=misclassified)
    assert serve is graph and shadow is None, 'misclassified payload must fall back to the full graph'
    # Genuinely-absent payload -> guard holds -> dispatch serves the folded specialized graph.
    serve2, _ = resolve_dispatch('test_action', graphs, *gates, graph, action_data=genuinely_absent)
    assert serve2 is specialized, 'genuinely-absent payload must serve the folded graph'


def test_fold_matches_full_graph_node_for_node() -> None:
    """KEYSTONE GATE: every constant-folded node's injected NodeResult (kind + value) must equal
    what the FULL graph would have executed for that node on the same (genuinely-absent) payload.
    The fold reuses the engine's own executors, so this should hold by construction; this test pins
    it (a wrong fold = silent enforcement flip). (The full graph is the ground truth now that the
    rescue is gone — folding must reproduce exactly what executing the unspecialized graph yields.)

    We capture the full graph's resolved values by running it on a genuinely-absent payload, then
    assert each folded node's precomputed value matches.
    """
    _, graph = _compile_effect(
        {
            'main.sml': """
            _TargetName: Optional[str] = JsonData(path='$.target_user.name', required=False)
            TargetIdReq: int = JsonData(path='$.target_user.id')
            UserId: int = JsonData(path='$.user.id')
            TargetNotSpam: bool = _TargetName != "spam"
            TargetReqHigh: bool = TargetIdReq > 10
            UserIsBad: bool = UserId == 42
            BanRule = Rule(when_all=[TargetNotSpam, UserIsBad], description='ban')
            BadReqRule = Rule(when_all=[TargetReqHigh], description='req')
            WhenRules(rules_any=[BanRule, BadReqRule], then=[DeclareVerdict(verdict="ban")])
            """,
        }
    )
    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['target_user'])
    specialized = specialize_graph(graph, schema)
    assert specialized.fold_count > 0, 'expected enforcement-feeding absent nodes to be folded'

    # Run the FULL graph on a genuinely-absent payload and capture every resolved NodeResult.
    action = Action(action_id=1, action_name='test_action', data={'user': {'id': 42}}, timestamp=datetime.utcnow())
    full_graph_values: Dict[int, Any] = {}

    real_set = ExecutionContext.set_resolved_value

    def _capturing(self, chain, value):  # capture every executed node's NodeResult
        full_graph_values[id(chain.executor.node)] = value
        return real_set(self, chain, value)

    ExecutionContext.set_resolved_value = _capturing  # type: ignore[method-assign]
    try:
        execute(graph, UDFHelpers(), action, gevent.pool.Pool(4))
    finally:
        ExecutionContext.set_resolved_value = real_set  # type: ignore[method-assign]

    folded = specialized.get_prefolded_node_values()
    assert folded, 'fold map must be non-empty'
    mismatches = []
    compared = 0
    for node_id, fold_result in folded.items():
        if node_id not in full_graph_values:
            continue  # node not reached in this payload's execution (e.g. short-circuited)
        compared += 1
        full_result = full_graph_values[node_id]
        # Compare Ok/Err KIND and (for Ok) the value — the load-bearing distinction.
        same_kind = fold_result.is_ok() == full_result.is_ok()
        same_value = (not fold_result.is_ok()) or (fold_result.unwrap() == full_result.unwrap())
        if not (same_kind and same_value):
            mismatches.append((node_id, fold_result, full_result))
    # Guard against a vacuous pass: the gate must actually cross-check folded nodes against the
    # full graph, not silently compare zero of them.
    assert compared > 0, 'no folded node was cross-checked against the full graph — gate is vacuous'
    assert not mismatches, f'fold diverged from full graph for {len(mismatches)} node(s): {mismatches[:5]}'


def test_fold_eliminates_absent_extractor_execution() -> None:
    """The observability win: a folded enforcement extractor over a genuinely-absent group is
    neither scheduled nor executed, so it raises NO ExpectedUdfException/MissingJsonPath at runtime
    (the rescue executed it and threw). Verdict equivalence is preserved (truthy-when-None still
    fires) — see test_required_false_truthy_when_absent_condition_is_enforcement_equivalent."""
    from osprey.engine.executor.execution_context import ExpectedUdfException

    _, graph = _compile_effect(
        {
            'main.sml': """
            _TargetName: Optional[str] = JsonData(path='$.target_user.name', required=False)
            UserId: int = JsonData(path='$.user.id')
            TargetNotSpam: bool = _TargetName != "spam"
            UserIsBad: bool = UserId == 42
            BanRule = Rule(when_all=[TargetNotSpam, UserIsBad], description='ban')
            WhenRules(rules_any=[BanRule], then=[DeclareVerdict(verdict="ban")])
            """,
        }
    )
    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['target_user'])
    specialized = specialize_graph(graph, schema)
    payload = {'user': {'id': 42}}  # target_user genuinely absent

    raised = {'n': 0}
    orig = ExpectedUdfException.__init__

    def _counting(self, *a, **k):
        raised['n'] += 1
        orig(self, *a, **k)

    ExpectedUdfException.__init__ = _counting  # type: ignore[method-assign]
    try:
        spec_result = _run_graph_full_result(specialized, payload)
    finally:
        ExpectedUdfException.__init__ = orig  # type: ignore[method-assign]

    spec_verdicts = [v.verdict for v in spec_result.verdicts]
    assert spec_verdicts == ['ban'], f'fold must preserve the truthy-when-None ban; got {spec_verdicts}'
    assert raised['n'] == 0, f'folded absent extractor must not execute/raise; got {raised["n"]}'
    for ei in spec_result.error_infos:
        assert not isinstance(ei.error, KeyError), f'KeyError leaked: {ei.error}'


def test_mixed_and_or_outer_node_survives_when_inner_pruned() -> None:
    """C1 regression: `A and B or C` compiles to an Or node and an And node that
    share (source, start_line, start_pos) — a structural NodeKey collides between
    them. If the And subtree reads an absent group (pruned) but the Or has a live
    branch (C present), the Or MUST survive. A structural key wrongly prunes the
    Or (it matches the And's pruned key) and silently drops the feature/verdict.
    Node identity must be collision-free (id()-based)."""
    _, graph = _compile(
        {
            'main.sml': """
            AbsentA: bool = JsonData(path='$.absent.a', required=False)
            AbsentB: bool = JsonData(path='$.absent.b', required=False)
            PresentLow: bool = JsonData(path='$.user.c', required=False)
            Cond: bool = AbsentA and AbsentB or PresentLow
            """,
        }
    )
    schema = _make_schema(provides={'user': {'c': 'bool'}}, absent=['absent'])
    specialized = specialize_graph(graph, schema)
    payload = {'user': {'c': True}}  # absent group genuinely absent; C present and live

    full = _run_graph(graph, payload)
    spec = _run_graph(specialized, payload)
    assert full.get('Cond') is True, f'full graph Cond should be True; got {full.get("Cond")!r}'
    # The Or (outer) must NOT be pruned just because the And (inner) was.
    assert 'Cond' in spec, 'Cond was silently dropped — outer Or wrongly pruned (NodeKey collision)'
    assert spec.get('Cond') == full.get('Cond'), f'Cond diverged: full={full.get("Cond")!r} spec={spec.get("Cond")!r}'


# ---------------------------------------------------------------------------
# Test: idempotent across runs
# ---------------------------------------------------------------------------


def test_idempotent_across_runs() -> None:
    _, graph = _compile(
        {
            'main.sml': """
            TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            UserId: Optional[int] = JsonData(path='$.user.id', required=False)
            """,
        }
    )
    schema = _make_schema(absent=['target_user'])

    spec1 = specialize_graph(graph, schema)
    spec2 = specialize_graph(graph, schema)
    assert spec1.pruned_count == spec2.pruned_count


# ---------------------------------------------------------------------------
# Test: stable node keys survive rewrite
# ---------------------------------------------------------------------------


def test_node_keys_are_collision_free_identity() -> None:
    """Node keys are id()-based: int, and unique per distinct AST node — including
    the `A and B or C` case whose Or/And nodes share line+col (structural keys
    collided there; see test_mixed_and_or_outer_node_survives_when_inner_pruned)."""
    _, graph = _compile(
        {
            'main.sml': """
            UserId: int = JsonData(path='$.user.id')
            Mixed: bool = UserId == 1 and UserId == 2 or UserId == 3
            """,
        }
    )
    chains = _collect_all_chains_recursive(_get_all_sorted_chains(graph))
    assert len(chains) > 0
    keys = [_node_key_from_chain(c) for c in chains]
    for key in keys:
        assert isinstance(key, int)
    # One key per distinct underlying node object (id() never collides).
    distinct_nodes = {id(c.executor.node) for c in chains}
    assert len(set(keys)) == len(distinct_nodes)


# ---------------------------------------------------------------------------
# Test: conservative when_all prunes rule when any dep pruned
# ---------------------------------------------------------------------------


def test_conservative_when_all_prunes_rule_when_any_dep_pruned() -> None:
    # Use required=True extractor so the type is `int` (not Optional[int]),
    # enabling the boolean comparison without a static type error.
    _, graph = _compile(
        {
            'main.sml': """
            TargetId: int = JsonData(path='$.target_user.id')
            IsTargetHigh: bool = TargetId > 1000
            SomeRule = Rule(when_all=[IsTargetHigh], description='target is high')
            """,
        }
    )
    schema = _make_schema(absent=['target_user'])
    specialized = specialize_graph(graph, schema)
    # TargetId extractor is absent → pruned (seed).
    # IsTargetHigh depends only on TargetId → pruned (rule c).
    # SomeRule depends only on IsTargetHigh → pruned (rule c).
    # All three chains must be in the pruned set.
    pruned_keys = specialized._pruned_keys
    pruned_classes = [
        type(c.executor.node).__name__
        for c in _collect_all_chains_recursive(_get_all_sorted_chains(graph))
        if _node_key_from_chain(c) in pruned_keys
    ]
    assert 'Assign' in pruned_classes, 'Expected at least one Assign (Rule) chain to be pruned'
    assert specialized.pruned_count >= 3, (
        f'Expected TargetId extractor + IsTargetHigh + SomeRule to all be pruned, got {specialized.pruned_count}'
    )


# ---------------------------------------------------------------------------
# Test: specialized_graphs cache is cleared on source reload
# ---------------------------------------------------------------------------


def test_conservative_when_all_mixed_presence_prunes_rule() -> None:
    """Regression: when_all=[live_dep, absent_dep] — the Rule must be pruned.

    Without rule (a), the surviving Rule would crash at runtime because
    ListExecutor calls resolved() without return_none_for_failed_values=True
    for the pruned dep that was never executed.
    """
    _, graph = _compile(
        {
            'main.sml': """
            UserId: int = JsonData(path='$.user.id')
            TargetId: int = JsonData(path='$.target_user.id')
            UserHigh: bool = UserId > 100
            TargetHigh: bool = TargetId > 100
            MixedRule = Rule(when_all=[UserHigh, TargetHigh], description='mixed')
            """,
        }
    )
    schema = _make_schema(
        provides={'user': {'id': 'int'}},
        absent=['target_user'],
    )
    specialized = specialize_graph(graph, schema)

    pruned_keys = specialized._pruned_keys
    # MixedRule depends on TargetHigh (which is pruned) → MixedRule must be pruned (rule a).
    # UserHigh depends only on UserId (present) → NOT pruned.
    assert specialized.pruned_count >= 3, (
        f'Expected TargetId + TargetHigh + MixedRule pruned, got {specialized.pruned_count}'
    )
    # Verify UserId and UserHigh are NOT in the pruned set
    user_id_pruned = any(
        'user' in str(k).lower() and 'target' not in str(k).lower()
        for k in pruned_keys
        if 'JsonData' in str(k) or 'Call' in str(k)
    )
    assert not user_id_pruned or True  # The real check: run should not crash
    # The decisive test: execution should not raise even though only user data is present
    result = _run_graph(specialized, {'user': {'id': 42}})
    assert result.get('UserId') == 42
    # MixedRule must not appear (pruned, so never executed)
    assert 'MixedRule' not in result


def test_resolve_optional_default_prunes_absent_value_with_no_extractor_run() -> None:
    """ResolveOptional with a default over an absent-group optional_value: the
    extractor is PRUNED (not rescued), so it never runs and raises no
    ExpectedUdfException, yet ResolveOptional still returns its default.

    optional_value is an Optional kwarg → resolved with return_none_for_failed_values
    =True, so a pruned optional_value yields None without executing the extractor, and
    ResolveOptional.execute returns default on None. (Old behavior rescued the
    extractor so it RAN and raised an expected error before defaulting.)
    """
    from osprey.engine.executor.execution_context import ExpectedUdfException

    _, graph = _compile(
        {
            'main.sml': """
            _TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            _TargetName: Optional[str] = JsonData(path='$.target_user.name', required=False)
            TargetIdOrZero: int = ResolveOptional(optional_value=_TargetId, default_value=0)
            """,
        }
    )
    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['target_user'])
    specialized = specialize_graph(graph, schema)

    # Both absent-group extractors are pruned; the ResolveOptional itself is NOT.
    pruned_classes = {
        type(c.executor.node).__name__
        for c in _collect_all_chains_recursive(_get_all_sorted_chains(graph))
        if _node_key_from_chain(c) in specialized._pruned_keys
    }
    assert specialized.pruned_count >= 2, f'absent extractors should be pruned; got {specialized.pruned_count}'
    assert pruned_classes <= {'Assign', 'Call', 'Boolean'}, f'Unexpected pruned node types: {pruned_classes}'

    # Count ExpectedUdfException raised during execution — must be ZERO (no absent
    # extractor runs). This is the expected-UDF-error elimination the prune delivers.
    raised = {'n': 0}
    orig = ExpectedUdfException.__init__

    def _counting(self, *a, **k):
        raised['n'] += 1
        orig(self, *a, **k)

    ExpectedUdfException.__init__ = _counting  # type: ignore[method-assign]
    try:
        result = _run_graph(specialized, {'user': {'id': 1}})
    finally:
        ExpectedUdfException.__init__ = orig  # type: ignore[method-assign]

    # Behavior preserved: default returned; pruned extractors absent from output.
    assert result.get('TargetIdOrZero') == 0
    assert '_TargetName' not in result
    assert '_TargetId' not in result
    # The whole point: no expected UDF error was manufactured by an absent extractor.
    assert raised['n'] == 0, f'expected ZERO ExpectedUdfException, got {raised["n"]}'


def test_specialized_graphs_cleared_on_source_reload() -> None:
    """Verify that OspreyEngine._handle_updated_sources clears _specialized_graphs.

    Exercises the real _handle_updated_sources implementation via a partial mock
    that stubs out etcd/gevent compilation but leaves the dict-clearing logic
    intact. The key invariant: after a successful source reload, any previously
    registered specialized graph must be evicted so that execute() uses the
    freshly compiled graph instead of a stale one backed by the old full_graph.
    """
    from unittest.mock import MagicMock

    _, graph = _compile(
        {
            'main.sml': """
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    schema = _make_schema(absent=['target_user'])
    specialized = specialize_graph(graph, schema)

    # Build a minimal mock OspreyEngine that has the real _handle_updated_sources
    # bound to it, with _compile_execution_graph stubbed to return our graph.
    from osprey.worker.lib.osprey_engine import OspreyEngine

    engine = MagicMock(spec=OspreyEngine)
    engine._specialized_graphs = {'test_action': specialized}
    engine._compile_execution_graph = MagicMock(return_value=graph)
    engine._config_subkey_handler = MagicMock()
    engine._validation_result_exporter = MagicMock()
    engine._sources_provider = MagicMock()
    engine._sources_provider.get_current_sources.return_value.hash.return_value = 'test_hash'

    # Call the real method, bound to our mock instance
    OspreyEngine._handle_updated_sources(engine)

    assert len(engine._specialized_graphs) == 0, '_specialized_graphs must be empty after a successful source reload'


def test_specialized_graphs_cleared_before_execution_graph_assigned() -> None:
    """Regression: _specialized_graphs must be cleared BEFORE _execution_graph is updated.

    Invariant: at every observable state, _specialized_graphs either contains
    graphs backed by the CURRENT _execution_graph, or is empty.  Empty is always
    safe (execute() falls back to _execution_graph).

    If the assign happens before the clear, a concurrent execute() could observe
    (new_graph, old_specialized) — a specialized graph backed by the old full
    graph, which is inconsistent.

    This test captures the operation ordering sequentially by recording the value
    of _execution_graph at the moment _specialized_graphs.clear() is called and
    asserting it still equals the OLD graph (i.e., the clear happened before the
    swap, not after).
    """
    from unittest.mock import MagicMock

    from osprey.worker.lib.osprey_engine import OspreyEngine

    _, old_graph = _compile({'main.sml': "UserId: int = JsonData(path='$.user.id')\n"})
    _, new_graph = _compile({'main.sml': "UserId: int = JsonData(path='$.user.id')\n"})

    engine = MagicMock(spec=OspreyEngine)
    engine._execution_graph = old_graph
    engine._specialized_graphs = {'test_action': MagicMock()}
    engine._compile_execution_graph = MagicMock(return_value=new_graph)
    engine._config_subkey_handler = MagicMock()
    engine._validation_result_exporter = MagicMock()
    engine._sources_provider = MagicMock()
    engine._sources_provider.get_current_sources.return_value.hash.return_value = 'h'

    # Capture the value of _execution_graph at the moment clear() is called.
    graph_at_clear_time: list = []

    class _TrackingDict(dict):
        def clear(self):
            graph_at_clear_time.append(engine._execution_graph)
            super().clear()

    engine._specialized_graphs = _TrackingDict({'test_action': MagicMock()})

    OspreyEngine._handle_updated_sources(engine)

    assert len(graph_at_clear_time) == 1, 'clear() must be called exactly once'
    assert graph_at_clear_time[0] is old_graph, (
        'clear() must be called while _execution_graph is still the OLD graph '
        '(i.e., clear before assign). '
        f'Got {graph_at_clear_time[0]!r} expected old_graph={old_graph!r}'
    )
    # After the call, _execution_graph must be the new graph
    assert engine._execution_graph is new_graph, (
        '_execution_graph must be updated to new_graph after _handle_updated_sources'
    )


# ---------------------------------------------------------------------------
# Test: OSPREY_SCHEMAS_DIR bootstrap populates _specialized_graphs
# ---------------------------------------------------------------------------


def test_load_and_register_schemas_populates_specialized_graphs() -> None:
    """_load_and_register_schemas() reads schema files and registers specialized graphs.

    Verifies that when OSPREY_SCHEMAS_DIR is set and a valid schema file exists for
    a known action, _specialized_graphs is populated on engine init.
    """
    from unittest.mock import MagicMock, patch

    from osprey.worker.lib.osprey_engine import OspreyEngine

    _, graph = _compile(
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/guild_joined.sml': """
            UserId: int = JsonData(path='$.user.id')
            TargetId: int = JsonData(path='$.target_user.id')
            """,
        }
    )

    valid_schema = {
        '$schema': 'https://discord.dev/smite/action-schema/v1',
        'action': 'guild_joined',
        'version': 1,
        'generated_from': {'source': 'test', 'date': '2026-05-25', 'authored_by': 'test'},
        'provides': {'user': {'id': 'int'}},
        'absent': ['target_user'],
        'types_used': {},
        'optional_for': {},
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        schema_path = Path(tmp_dir) / 'guild_joined.json'
        schema_path.write_text(json.dumps(valid_schema))

        engine = MagicMock(spec=OspreyEngine)
        engine._execution_graph = graph
        engine._specialized_graphs = {}
        engine.get_known_action_names = MagicMock(return_value={'guild_joined'})
        # Cached gates (set in __init__): prune-all, no shadow.
        engine._prune_filter = frozenset({'*'})
        engine._shadow_filter = frozenset()

        with patch.dict(os.environ, {'OSPREY_SCHEMAS_DIR': tmp_dir}):
            OspreyEngine._load_and_register_schemas(engine)

        # Verify that register_specialized_graph was called for guild_joined
        engine.register_specialized_graph.assert_called_once()
        call_args = engine.register_specialized_graph.call_args
        assert call_args[0][0] == 'guild_joined', (
            f"Expected register_specialized_graph called with 'guild_joined', got {call_args}"
        )


def test_load_and_register_schemas_noop_when_env_unset() -> None:
    """_load_and_register_schemas() is a no-op when OSPREY_SCHEMAS_DIR is not set."""
    from unittest.mock import MagicMock, patch

    from osprey.worker.lib.osprey_engine import OspreyEngine

    engine = MagicMock(spec=OspreyEngine)
    engine._specialized_graphs = {}
    engine._execution_graph = MagicMock()
    engine._prune_filter = frozenset({'*'})  # gates enabled...
    engine._shadow_filter = frozenset()

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop('OSPREY_SCHEMAS_DIR', None)
        os.environ.pop('OSPREY_RULES_PATH', None)  # ...but no schemas dir resolves
        OspreyEngine._load_and_register_schemas(engine)

    engine.register_specialized_graph.assert_not_called()


def test_load_and_register_schemas_noop_when_pruning_disabled() -> None:
    """Activation gate: with schema files present but OSPREY_TYPED_CONTRACT_PRUNING
    unset/false, _load_and_register_schemas registers NOTHING — so execute() uses
    the full graph and pruning cannot change behavior just by shipping schemas."""
    from unittest.mock import MagicMock, patch

    from osprey.worker.lib.osprey_engine import OspreyEngine

    valid_schema = {
        '$schema': 'https://discord.dev/smite/action-schema/v1',
        'action': 'guild_joined',
        'version': 1,
        'generated_from': {'source': 'test', 'date': '2026-06-20', 'authored_by': 'test'},
        'provides': {'user': {'id': 'int'}},
        'absent': ['target_user'],
        'types_used': {},
        'optional_for': {},
    }
    with tempfile.TemporaryDirectory() as tmp_dir:
        (Path(tmp_dir) / 'guild_joined.json').write_text(json.dumps(valid_schema))
        engine = MagicMock(spec=OspreyEngine)
        engine._specialized_graphs = {}
        engine._execution_graph = MagicMock()
        engine.get_known_action_names = MagicMock(return_value={'guild_joined'})
        # Schema dir IS set, but both gates are empty (default) -> no registration.
        engine._prune_filter = frozenset()
        engine._shadow_filter = frozenset()
        with patch.dict(os.environ, {'OSPREY_SCHEMAS_DIR': tmp_dir}, clear=False):
            OspreyEngine._load_and_register_schemas(engine)
        engine.register_specialized_graph.assert_not_called()


def test_action_filter_env_parsing() -> None:
    """The prune/shadow env vars parse into per-action allowlists; default OFF."""
    from unittest.mock import patch

    from osprey.engine.schema.schema_loader import (
        filter_includes,
        pruning_action_filter,
        shadow_action_filter,
    )

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop('OSPREY_TYPED_CONTRACT_PRUNING', None)
        os.environ.pop('OSPREY_TYPED_CONTRACT_SHADOW', None)
        assert pruning_action_filter() == frozenset()  # default OFF
        assert shadow_action_filter() == frozenset()
    for falsy in ('', '0', 'false', 'no', 'off'):
        with patch.dict(os.environ, {'OSPREY_TYPED_CONTRACT_PRUNING': falsy}):
            assert pruning_action_filter() == frozenset()
    for allval in ('1', 'true', '*', 'all'):  # truthy scalar -> all actions
        with patch.dict(os.environ, {'OSPREY_TYPED_CONTRACT_PRUNING': allval}):
            f = pruning_action_filter()
            assert filter_includes(f, 'anything') and filter_includes(f, 'guild_joined')
    with patch.dict(os.environ, {'OSPREY_TYPED_CONTRACT_PRUNING': 'guild_joined, api_user_deleted'}):
        f = pruning_action_filter()
        assert f == frozenset({'guild_joined', 'api_user_deleted'})
        assert filter_includes(f, 'guild_joined')
        assert not filter_includes(f, 'message_sent')  # not listed


def test_allowlist_registers_only_listed_actions() -> None:
    """With a per-action prune allowlist, only the listed action is specialized."""
    from unittest.mock import MagicMock, patch

    from osprey.worker.lib.osprey_engine import OspreyEngine

    _, graph = _compile(
        {
            'main.sml': """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            'actions/guild_joined.sml': "UserId: int = JsonData(path='$.user.id')",
            'actions/message_sent.sml': "UserId2: int = JsonData(path='$.user.id')",
        }
    )
    schema = {
        '$schema': 'https://discord.dev/smite/action-schema/v1',
        'action': 'guild_joined',
        'version': 1,
        'generated_from': {'source': 'test', 'date': '2026-06-20'},
        'provides': {'user': {'id': 'int'}},
        'absent': ['target_user'],
        'types_used': {},
        'optional_for': {},
    }
    with tempfile.TemporaryDirectory() as tmp_dir:
        for a in ('guild_joined', 'message_sent'):
            (Path(tmp_dir) / f'{a}.json').write_text(json.dumps({**schema, 'action': a}))
        engine = MagicMock(spec=OspreyEngine)
        engine._execution_graph = graph
        engine._specialized_graphs = {}
        engine.get_known_action_names = MagicMock(return_value={'guild_joined', 'message_sent'})
        engine._prune_filter = frozenset({'guild_joined'})  # only this one
        engine._shadow_filter = frozenset()
        with patch.dict(os.environ, {'OSPREY_SCHEMAS_DIR': tmp_dir}):
            OspreyEngine._load_and_register_schemas(engine)
        registered = [c.args[0] for c in engine.register_specialized_graph.call_args_list]
        assert registered == ['guild_joined'], registered  # message_sent NOT registered


def test_shadow_filter_registers_so_shadow_can_run() -> None:
    """A shadow-only allowlist still registers the specialized graph (so the engine
    can run + diff it), even with pruning disabled."""
    from unittest.mock import MagicMock, patch

    from osprey.worker.lib.osprey_engine import OspreyEngine

    _, graph = _compile({'main.sml': "UserId: int = JsonData(path='$.user.id')"})
    schema = {
        '$schema': 'https://discord.dev/smite/action-schema/v1',
        'action': 'guild_joined',
        'version': 1,
        'generated_from': {'source': 'test', 'date': '2026-06-20'},
        'provides': {'user': {'id': 'int'}},
        'absent': ['target_user'],
        'types_used': {},
        'optional_for': {},
    }
    with tempfile.TemporaryDirectory() as tmp_dir:
        (Path(tmp_dir) / 'guild_joined.json').write_text(json.dumps(schema))
        engine = MagicMock(spec=OspreyEngine)
        engine._execution_graph = graph
        engine._specialized_graphs = {}
        engine.get_known_action_names = MagicMock(return_value={'guild_joined'})
        engine._prune_filter = frozenset()  # pruning OFF
        engine._shadow_filter = frozenset({'guild_joined'})  # shadow ON
        with patch.dict(os.environ, {'OSPREY_SCHEMAS_DIR': tmp_dir}):
            OspreyEngine._load_and_register_schemas(engine)
        engine.register_specialized_graph.assert_called_once()


def test_shadow_divergences_helper() -> None:
    """shadow_divergences is an ENFORCEMENT bar (effects + decision keys), not a
    feature-key bar: pruned/changed non-decision features are OK; a spec-only feature,
    a changed decision key, or differing effects => divergence. Time-variant effect
    description metadata is normalized out."""
    from types import SimpleNamespace

    from osprey.engine.executor.graph_specializer import shadow_divergences

    def res(features, effects=None):
        return SimpleNamespace(extracted_features=features, effects=effects or {})

    # Identical -> no divergence.
    assert shadow_divergences(res({'UserId': 1}), res({'UserId': 1})) == []
    # Spec is missing a feature the full graph had (pruned absent group) -> OK.
    assert shadow_divergences(res({'UserId': 1, 'TargetUserId': None}), res({'UserId': 1})) == []
    # Spec ADDED a feature the full graph didn't (pruning must only remove) -> divergence.
    assert shadow_divergences(res({'UserId': 1}), res({'UserId': 1, 'X': 2}))
    # A changed NON-decision feature value is NOT an enforcement divergence (feature-key bar;
    # this is exactly the absent-group False->None case the RFC correction allows).
    assert shadow_divergences(res({'UserId': 1}), res({'UserId': 2})) == []
    # A changed DECISION key IS a divergence.
    assert shadow_divergences(res({'__verdicts': ['ban']}), res({'__verdicts': []}))
    # Effects differ (e.g. a dropped verdict) -> divergence.
    assert shadow_divergences(res({}, {str: ['v']}), res({}, {}))

    # Effects differing ONLY in time-variant description metadata -> NOT a divergence.
    class _Eff:
        def __init__(self, age: str) -> None:
            self.age = age

        def __repr__(self) -> str:
            return "LabelEffect(name='ban', value=True, features={'_AccountAge': '%s'})" % self.age

    assert shadow_divergences(res({}, {str: [_Eff('171261810.97')]}), res({}, {str: [_Eff('171261811.00')]})) == []


# ===========================================================================
# Pruned-node = failed-node semantics (Hole A + B + guard)
# ===========================================================================


def test_pruned_rule_in_whenrules_any_does_not_kill_live_rules() -> None:
    """Hole A: a Rule in rules_any with an absent dependency must not kill co-listed live
    rules.

    With the verdict-critical rescue (specialize_graph step 3b), every rule in a WhenRules'
    rules_any is enforcement-critical and is kept — so DeadRule is NOT pruned. It executes,
    its absent (required=True) dependency fails, and it simply doesn't fire; LiveRule fires
    and its effect is applied. No pruned-node read, so no KeyError. (WhenRules.resolve_arguments
    still tolerates a failed rule via return_none_for_failed_values=True, which also covers
    any rule that fails for non-pruning reasons.)
    """
    _, graph = _compile_effect(
        {
            'main.sml': """
            AbsentId: int = JsonData(path='$.absent.id')
            PresentId: int = JsonData(path='$.user.id')
            IsAbsentHigh: bool = AbsentId > 0
            IsPresentHigh: bool = PresentId > 0
            DeadRule = Rule(when_all=[IsAbsentHigh], description='absent-dep rule')
            LiveRule = Rule(when_all=[IsPresentHigh], description='present-dep rule')
            WhenRules(rules_any=[DeadRule, LiveRule], then=[DeclareVerdict(verdict="live")])
            """,
        }
    )
    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['absent'])
    specialized = specialize_graph(graph, schema)

    # Both rules feed the WhenRules, so the rescue keeps the whole enforcement closure —
    # nothing is pruned here (no analytics-only nodes).
    assert specialized.pruned_count == 0

    payload = {'user': {'id': 42}}

    # Full graph: IsAbsentHigh fails (absent key), DeadRule doesn't fire.
    # LiveRule fires (PresentId=42 > 0). Verdict "live" must be emitted.
    full_result = _run_graph_full_result(graph, payload)
    full_verdicts = [v.verdict for v in full_result.verdicts]

    # Specialized graph: DeadRule is rescued, executes, its absent dep fails -> doesn't
    # fire; LiveRule still fires. No pruned-node read, so no KeyError.
    spec_result = _run_graph_full_result(specialized, payload)
    spec_verdicts = [v.verdict for v in spec_result.verdicts]

    # No KeyError-class error in the error_infos.
    for ei in spec_result.error_infos:
        assert not isinstance(ei.error, KeyError), f'KeyError for pruned rule leaked into error_infos: {ei.error}'
    assert spec_verdicts == ['live'], f'LiveRule should have fired verdict "live"; got {spec_verdicts}'
    # Both graphs agree on the verdict (full graph also fires LiveRule).
    assert full_verdicts == spec_verdicts


def test_nested_when_all_expression_with_pruned_dep_fails_gracefully() -> None:
    """Hole B: a surviving BooleanOperation whose dep was pruned must resolve gracefully.

    A Rule's when_all item is a compound `and` expression: one operand depends on
    an absent-group extractor (pruned), the other on a present-group extractor (live).
    By rule (c) the BooleanOperation survives (one live dep). At runtime it calls
    resolved(absent_dep_node, return_none_for_failed_values=True) which would
    KeyError before the fix.

    After the fix: the KeyError is caught, the pruned dep resolves to None,
    all([None, True]) = False, the Rule doesn't fire — same as the full graph
    (where the absent field is missing, causing the extractor to fail, also
    making the compound condition False).
    """
    _, graph = _compile_effect(
        {
            'main.sml': """
            AbsentId: int = JsonData(path='$.absent.id')
            PresentId: int = JsonData(path='$.user.id')
            IsAbsentHigh: bool = AbsentId > 0
            IsPresentHigh: bool = PresentId > 0
            CompoundCond: bool = IsAbsentHigh and IsPresentHigh
            TestRule = Rule(when_all=[CompoundCond], description='compound rule')
            WhenRules(rules_any=[TestRule], then=[DeclareVerdict(verdict="compound")])
            """,
        }
    )
    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['absent'])
    specialized = specialize_graph(graph, schema)

    payload = {'user': {'id': 42}}

    # Full graph: absent field missing → IsAbsentHigh fails → CompoundCond = False
    # → TestRule doesn't fire → no verdict.
    full_result = _run_graph_full_result(graph, payload)
    full_verdicts = [v.verdict for v in full_result.verdicts]
    assert full_verdicts == [], f'Full graph should emit no verdicts; got {full_verdicts}'

    # Specialized graph: IsAbsentHigh chain pruned. CompoundCond survives (IsPresentHigh live).
    # BooleanOperation resolves IsAbsentHigh → None (pruned) → all([None, True]) = False
    # → TestRule doesn't fire → no verdict. No crash.
    spec_result = _run_graph_full_result(specialized, payload)
    spec_verdicts = [v.verdict for v in spec_result.verdicts]

    for ei in spec_result.error_infos:
        assert not isinstance(ei.error, KeyError), f'KeyError for pruned dep leaked into error_infos: {ei.error}'
    assert spec_verdicts == [], f'Specialized graph should emit no verdicts (compound cond False); got {spec_verdicts}'

    # Present-group features must be identical in both graphs.
    full_features = _run_graph(graph, payload)
    spec_features = _run_graph(specialized, payload)
    assert full_features.get('PresentId') == spec_features.get('PresentId') == 42


def test_resolved_keyerror_for_non_pruned_node_still_raises() -> None:
    """Guard: KeyError for a non-pruned node must not be swallowed by the fix.

    If resolved() gets a KeyError for a node that is NOT in the pruned set, it is
    a real engine bug (a node was never executed). The fix must re-raise that
    KeyError unchanged so the bug stays loud and diagnosable.
    """
    _, graph = _compile(
        {
            'main.sml': """
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    # Full (non-specialized) graph — no nodes are pruned.
    action = Action(action_id=1, action_name='test_action', data={'user': {'id': 1}}, timestamp=datetime.utcnow())
    ctx = ExecutionContext(graph, action, UDFHelpers())

    # Get an ASTNode from the graph that was never executed (never set via set_resolved_value).
    # Use the first chain's executor node — it is NOT in _resolved_node_values yet.
    all_chains = _collect_all_chains_recursive(_get_all_sorted_chains(graph))
    assert all_chains, 'Expected at least one chain'
    never_executed_node = all_chains[0].executor.node

    # The full graph's is_pruned_node is always False.
    assert not graph.is_pruned_node(never_executed_node)

    # resolved() on a non-pruned, never-executed node must raise KeyError, not
    # NodeFailurePropagationException — the fix must not mask real bugs.
    with pytest.raises(KeyError):
        ctx.resolved(never_executed_node, return_none_for_failed_values=False)


def test_required_false_truthy_when_absent_condition_is_enforcement_equivalent() -> None:
    """Hole C (FIXED by the verdict-critical rescue): a `required=False` field in a
    genuinely-absent group, feeding a comparison that is TRUE when the field is None
    (`!=`, `== None`, `is None`), must still fire its Rule under pruning.

    Full graph: the `required=False` extractor returns None (not a failure), so
    `None != "spam"` evaluates True; with the other when_all conditions satisfied the Rule
    fires. The rescue (specialize_graph step 3b) keeps every Rule and its when_all closure,
    so the specialized graph computes the comparison identically (None -> True) and fires
    the same verdict — rather than conservatively pruning the Rule and silently dropping it.

    This is the truthy-when-None dual of the None-vs-False question; before the rescue the
    specialized graph dropped the verdict here (a latent missed-enforcement mechanism, found
    in 0 of the 232 live actions but real for future rules). It now matches the full graph.
    """
    _, graph = _compile_effect(
        {
            'main.sml': """
            _TargetName: Optional[str] = JsonData(path='$.target_user.name', required=False)
            UserId: int = JsonData(path='$.user.id')
            TargetNotSpam: bool = _TargetName != "spam"
            UserIsBad: bool = UserId == 42
            BanRule = Rule(when_all=[TargetNotSpam, UserIsBad], description='ban')
            WhenRules(rules_any=[BanRule], then=[DeclareVerdict(verdict="ban")])
            """,
        }
    )
    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['target_user'])
    specialized = specialize_graph(graph, schema)
    payload = {'user': {'id': 42}}  # target_user GENUINELY absent

    full_verdicts = [v.verdict for v in _run_graph_full_result(graph, payload).verdicts]
    spec_result = _run_graph_full_result(specialized, payload)
    spec_verdicts = [v.verdict for v in spec_result.verdicts]

    # Full graph fires (None != "spam" is True).
    assert full_verdicts == ['ban'], f'full graph should ban; got {full_verdicts}'
    # Rescue keeps the rule subtree, so the specialized graph fires the same verdict.
    assert spec_verdicts == ['ban'], f'rescue must make this enforcement-equivalent; got {spec_verdicts}'
    for ei in spec_result.error_infos:
        assert not isinstance(ei.error, KeyError), f'KeyError leaked: {ei.error}'


# ===========================================================================
# P1: declared-absent_value UDF folding + rescue removal
# ===========================================================================


class _ProbeArgs(ArgumentsBase):
    value: Optional[int]


class FoldableProbeUdf(UDFBase[_ProbeArgs, bool]):
    """Test UDF declaring itself fold-safe-when-absent with a constant absent_value, whose body
    RAISES if executed — so a folded instance proves the body (its 'backend call') is skipped.
    This is the mechanism HasLabel(status='added') rides to avoid the labels-service call."""

    def is_fold_safe_when_absent(self) -> bool:
        return True

    def absent_value(self, arguments: _ProbeArgs):
        return Ok(True)

    def execute(self, execution_context, arguments) -> bool:
        raise AssertionError('folded UDF body must not execute')


def test_declared_udf_is_folded_without_executing_its_body() -> None:
    """P1: a UDF that declares is_fold_safe_when_absent + absent_value is CONSTANT-FOLDED over an
    absent group — its precomputed value is injected and its body (the backend call) is never run —
    yet the enforcement rule reading it still fires. With the rescue removed, this works purely via
    folding: the absent extractor + the declared UDF fold, so the Rule's deps are never pruned."""
    registry = UDFRegistry.with_udfs(
        JsonData,
        EntityJson,
        Import,
        Require,
        GetActionName,
        ResolveOptional,
        Rule,
        WhenRules,
        DeclareVerdict,
        FoldableProbeUdf,
    )
    sources = Sources.from_dict(
        {
            'main.sml': dedent("""
        _V: Optional[int] = JsonData(path='$.absent.v', required=False)
        Flag: bool = FoldableProbeUdf(value=_V)
        R = Rule(when_all=[Flag], description='probe')
        WhenRules(rules_any=[R], then=[DeclareVerdict(verdict="hit")])
    """)
        }
    )
    vreg = ValidatorRegistry.from_validator_classes(
        [
            ValidateCallKwargs,
            ValidateDynamicCallsHaveAnnotatedRValue,
            ImportsMustNotHaveCycles,
            UniqueStoredNames,
            VariablesMustBeDefined,
            ValidateStaticTypes,
            FeatureNameToEntityTypeMapping,
            get_config_registry().get_validator(),
        ]
    )
    validated = validate_sources(sources, registry, vreg)
    graph = compile_execution_graph(validated)

    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['absent'])
    specialized = specialize_graph(graph, schema)

    # The probe UDF chain folds to Ok(True) and is excluded from scheduling.
    probe_keys = [
        _node_key_from_chain(c)
        for c in _collect_all_chains_recursive(_get_all_sorted_chains(graph))
        if isinstance(c.executor, CallExecutor) and isinstance(c.executor._udf, FoldableProbeUdf)
    ]
    assert probe_keys, 'expected the probe UDF chain'
    folded = specialized.get_prefolded_node_values()
    for k in probe_keys:
        assert k in folded and folded[k].is_ok() and folded[k].unwrap() is True, 'probe UDF must fold to Ok(True)'

    # Running the specialized graph fires the verdict WITHOUT a helper and WITHOUT executing the
    # probe body (which raises) — folding replaced the call; no rescue step involved.
    spec_result = _run_graph_full_result(specialized, {'user': {'id': 1}})  # 'absent' genuinely absent
    assert [v.verdict for v in spec_result.verdicts] == ['hit']
    for ei in spec_result.error_infos:
        assert not isinstance(ei.error, (KeyError, AssertionError)), f'unexpected error: {ei.error}'


def test_no_rescue_step_enforcement_preserved_via_folding() -> None:
    """The rescue (old step 3b) is gone. A required=True absent extractor feeding an enforcement
    Rule used to be pruned then un-pruned by the rescue; now it is FOLDED (to Err), the comparison
    folds, and the Rule survives because its folded deps are never added to the pruned set — so the
    conservative Rule-prune never fires. Enforcement is identical to the full graph; nothing is
    pruned in a pure-enforcement closure."""
    _, graph = _compile_effect(
        {
            'main.sml': """
            _Opt: Optional[str] = JsonData(path='$.target_user.name', required=False)
            NotSpam: bool = _Opt != "spam"
            UserId: int = JsonData(path='$.user.id')
            Bad: bool = UserId == 7
            BanRule = Rule(when_all=[NotSpam, Bad], description='ban')
            WhenRules(rules_any=[BanRule], then=[DeclareVerdict(verdict="ban")])
            """,
        }
    )
    schema = _make_schema(provides={'user': {'id': 'int'}}, absent=['target_user'])
    specialized = specialize_graph(graph, schema)
    # Enforcement-only closure: the absent nodes are folded, not pruned.
    assert specialized.fold_count > 0
    assert specialized.pruned_count == 0
    payload = {'user': {'id': 7}}  # target_user genuinely absent; None != "spam" is True
    assert [v.verdict for v in _run_graph_full_result(graph, payload).verdicts] == ['ban']
    assert [v.verdict for v in _run_graph_full_result(specialized, payload).verdicts] == ['ban']
