"""Tests for ExecutionResult.get_verdicts_pb2_proto().

Verifies that LabelAdd effects are synthesised into entity verdict strings so
that callers reading entity_has_label() on a sync ProcessAction response see
labels applied during this action.
"""
from datetime import datetime
from typing import Any, Dict, Mapping, Sequence, Type

import pytest
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.engine.language_types.effects import EffectBase
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelEffect, LabelStatus
from osprey.engine.language_types.rules import RuleT
from osprey.engine.language_types.verdicts import VerdictEffect


_ENTITY = EntityT(type='User', id=12345)
_LABEL_NAME = 'require_verified_phone_then_email'


def _make_action() -> Action:
    return Action(
        action_id=1,
        action_name='test_action',
        data={},
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
    )


def _make_result(effects: Mapping[Type[EffectBase], Sequence[EffectBase]]) -> ExecutionResult:
    return ExecutionResult(
        extracted_features={},
        action=_make_action(),
        effects=effects,
        error_infos=[],
    )


def _make_label_effect(
    status: LabelStatus = LabelStatus.ADDED,
    suppressed: bool = False,
    dependent_rule: RuleT | None = None,
) -> LabelEffect:
    return LabelEffect(
        entity=_ENTITY,
        status=status,
        name=_LABEL_NAME,
        suppressed=suppressed,
        dependent_rule=dependent_rule,
    )


def _make_rule(value: bool) -> RuleT:
    return RuleT(name='test_rule', value=value, description='', features={})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_effects_produces_empty_verdicts():
    """Regression: no effects → no verdicts in output."""
    result = _make_result({})
    pb = result.get_verdicts_pb2_proto()
    assert list(pb.verdicts) == []


def test_verdict_effects_only_unchanged():
    """Existing VerdictEffects still appear in output when no LabelEffects are present."""
    ve = VerdictEffect(verdict='User/999/some_verdict')
    result = _make_result({VerdictEffect: [ve]})
    pb = result.get_verdicts_pb2_proto()
    assert list(pb.verdicts) == ['User/999/some_verdict']


def test_label_effect_added_not_suppressed_synthesised():
    """ADDED, not suppressed, no dependent_rule → verdict string synthesised."""
    le = _make_label_effect(status=LabelStatus.ADDED, suppressed=False)
    result = _make_result({LabelEffect: [le]})
    pb = result.get_verdicts_pb2_proto()
    assert f'User/12345/{_LABEL_NAME}' in list(pb.verdicts)


def test_label_effect_suppressed_not_synthesised():
    """Suppressed LabelEffect → NOT synthesised."""
    le = _make_label_effect(suppressed=True)
    result = _make_result({LabelEffect: [le]})
    pb = result.get_verdicts_pb2_proto()
    assert list(pb.verdicts) == []


def test_label_effect_dependent_rule_false_not_synthesised():
    """dependent_rule.value=False → NOT synthesised."""
    le = _make_label_effect(dependent_rule=_make_rule(value=False))
    result = _make_result({LabelEffect: [le]})
    pb = result.get_verdicts_pb2_proto()
    assert list(pb.verdicts) == []


def test_label_effect_dependent_rule_true_synthesised():
    """dependent_rule.value=True → verdict synthesised."""
    le = _make_label_effect(dependent_rule=_make_rule(value=True))
    result = _make_result({LabelEffect: [le]})
    pb = result.get_verdicts_pb2_proto()
    assert f'User/12345/{_LABEL_NAME}' in list(pb.verdicts)


def test_label_effect_removed_not_synthesised():
    """REMOVED LabelEffect → NOT synthesised (only ADDED emits positive-signal verdicts)."""
    le = _make_label_effect(status=LabelStatus.REMOVED)
    result = _make_result({LabelEffect: [le]})
    pb = result.get_verdicts_pb2_proto()
    assert list(pb.verdicts) == []


def test_mix_of_verdict_and_label_effects():
    """Both VerdictEffects and synthesised LabelEffect verdicts appear together."""
    ve = VerdictEffect(verdict='User/999/some_verdict')
    le = _make_label_effect(status=LabelStatus.ADDED, suppressed=False)
    result = _make_result({VerdictEffect: [ve], LabelEffect: [le]})
    pb = result.get_verdicts_pb2_proto()
    verdicts = list(pb.verdicts)
    assert 'User/999/some_verdict' in verdicts
    assert f'User/12345/{_LABEL_NAME}' in verdicts
    assert len(verdicts) == 2
