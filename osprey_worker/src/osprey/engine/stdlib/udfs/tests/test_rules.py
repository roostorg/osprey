import dataclasses
import json
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Mapping, Sequence

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import (
    CheckFailureFunction,
    ExecuteFunction,
    ExecuteWithResultFunction,
    RunValidationFunction,
)
from osprey.engine.executor.execution_context import (
    ExecutionContext,
)
from osprey.engine.language_types.entities import EntityT
from osprey.engine.stdlib.udfs.entity import Entity
from osprey.engine.stdlib.udfs.labels import LabelAdd, LabelRemove
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.stdlib.udfs.time_delta import TimeDelta
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.osprey_shared.labels import EntityLabelMutation, LabelStatus
from osprey.worker.sinks.sink.output_sink import _get_label_effects_from_result


class FailingUdf(UDFBase[ArgumentsBase, bool]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> bool:
        raise ValueError('intentional failure')


class FailingString(UDFBase[ArgumentsBase, str]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
        raise ValueError('intentional failure')


pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(Entity, Rule, WhenRules, LabelAdd, LabelRemove, TimeDelta, FailingUdf, FailingString)
    ),
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
]


def test_unary_rules(execute: ExecuteFunction) -> None:
    rule = """
    A = Rule(when_all=[False], description='..')
    B = Rule(when_all=[not A], description='..')
    C = Rule(when_all=[1 > -1], description='..')
    """
    data = execute(rule)
    assert data == {'A': False, 'B': True, 'C': True}


def test_rule_requires_all_inputs_to_be_true(execute: ExecuteFunction) -> None:
    data = execute(
        """
        R1 = Rule(when_all=[True, False, True], description='')
        R2 = Rule(when_all=[True, True, True], description='')
        """
    )
    assert data == {'R1': False, 'R2': True}


def test_when_rules(execute_with_result: ExecuteWithResultFunction) -> None:
    now = datetime.now()
    result = execute_with_result(
        {
            'main.sml': """
                RFailed1 = Rule(when_all=[FailingUdf()], description='I always fail')
                RFailed2 = Rule(when_all=[FailingUdf()], description='I always fail')
                RSimple1 = Rule(when_all=[True], description='simple rule 1')
                RSimple2 = Rule(when_all=[True], description='simple rule 2')
                RSimple3 = Rule(when_all=[True], description='simple rule 3')
                UserName = 'Wumpus'
                RWithFString = Rule(when_all=[True], description=f'fstring rule description with name {UserName}')
                RFalse = Rule(when_all=[False], description='should not show up')
                E1 = Entity(type='MyEntity', id='entity 1')
                E2 = Entity(type='MyEntity', id='entity 2')
                E3 = Entity(type='MyEntity', id=FailingString())

                WhenRules(
                    rules_any=[RSimple3],
                    then=[
                        LabelAdd(entity=E1, label='garply', expires_after=TimeDelta(seconds=30)),
                        # Failed LabelAdd (because it depends on E3 which fails) -- should not stop
                        # the previous line from executing
                        LabelAdd(entity=E3, label='qux', expires_after=TimeDelta(seconds=30)),
                    ],
                )
                WhenRules(
                    rules_any=[
                        # A rule that fails shouldn't break this.
                        RFailed1,
                        # Multiple types of rules
                        RSimple1,
                        RWithFString,
                        # Should not show up in output
                        RFalse,
                    ],
                    then=[
                        LabelAdd(entity=E1, label='foo', expires_after=TimeDelta(seconds=45)),
                        LabelRemove(entity=E1, label='bar'),
                    ],
                )
                WhenRules(
                    rules_any=[RSimple1, RFailed2],
                    then=[
                        LabelAdd(entity=E2, label='garply', expires_after=TimeDelta(seconds=30)),
                    ],
                )
                WhenRules(
                    rules_any=[RSimple2],
                    then=[
                        # Merge outputs for same entity from multiple different WhenRules
                        LabelAdd(entity=E1, label='baz', expires_after=TimeDelta(minutes=5)),
                        # Merge outputs for same entity *and* label from multiple different WhenRules
                        LabelAdd(
                            entity=E1,
                            label='foo',
                            expires_after=TimeDelta(minutes=3),
                        ),
                        # Split outputs for different entities from same WhenRules
                        LabelRemove(entity=E2, label='garply'),
                        LabelRemove(entity=E2, label='qux'),
                    ],
                )
                WhenRules(
                    rules_any=[RWithFString],
                    then=[
                        LabelAdd(entity=E2, label='garply'),
                    ],
                )
                WhenRules(
                    rules_any=[RFalse],
                    then=[
                        # Should not show up
                        LabelAdd(entity=E1, label='bad label', expires_after=TimeDelta(days=1)),
                    ],
                )
            """,
            'config.yaml': json.dumps(
                {
                    'labels': {
                        label_name: {'valid_for': ['MyEntity']}
                        for label_name in ('foo', 'bar', 'baz', 'garply', 'qux', 'bad label')
                    }
                }
            ),
        },
        action_time=now,
    )

    expected: Mapping[EntityT[Any], Sequence[EntityLabelMutation]] = {
        EntityT(type='MyEntity', id='entity 1'): [
            EntityLabelMutation(
                label_name='foo',
                reason_name='RSimple1',
                status=LabelStatus.ADDED,
                pending=False,
                description='simple rule 1',
                features={},
                expires_at=now + timedelta(seconds=45),
            ),
            EntityLabelMutation(
                label_name='foo',
                reason_name='RWithFString',
                status=LabelStatus.ADDED,
                pending=False,
                description='fstring rule description with name {UserName}',
                features={'UserName': 'Wumpus'},
                expires_at=now + timedelta(seconds=45),
            ),
            EntityLabelMutation(
                label_name='bar',
                reason_name='RSimple1',
                status=LabelStatus.REMOVED,
                pending=False,
                description='simple rule 1',
                features={},
                expires_at=None,
            ),
            EntityLabelMutation(
                label_name='bar',
                reason_name='RWithFString',
                status=LabelStatus.REMOVED,
                pending=False,
                description='fstring rule description with name {UserName}',
                features={'UserName': 'Wumpus'},
                expires_at=None,
            ),
            EntityLabelMutation(
                label_name='baz',
                reason_name='RSimple2',
                status=LabelStatus.ADDED,
                pending=False,
                description='simple rule 2',
                features={},
                expires_at=now + timedelta(minutes=5),
            ),
            EntityLabelMutation(
                label_name='foo',
                reason_name='RSimple2',
                status=LabelStatus.ADDED,
                pending=False,
                description='simple rule 2',
                features={},
                expires_at=now + timedelta(minutes=3),
            ),
            EntityLabelMutation(
                label_name='garply',
                reason_name='RSimple3',
                status=LabelStatus.ADDED,
                pending=False,
                description='simple rule 3',
                features={},
                expires_at=now + timedelta(seconds=30),
            ),
        ],
        EntityT(type='MyEntity', id='entity 2'): [
            EntityLabelMutation(
                label_name='garply',
                reason_name='RSimple1',
                status=LabelStatus.ADDED,
                pending=False,
                description='simple rule 1',
                features={},
                expires_at=now + timedelta(seconds=30),
            ),
            EntityLabelMutation(
                label_name='garply',
                reason_name='RSimple2',
                status=LabelStatus.REMOVED,
                pending=False,
                description='simple rule 2',
                features={},
                expires_at=None,
            ),
            EntityLabelMutation(
                label_name='qux',
                reason_name='RSimple2',
                status=LabelStatus.REMOVED,
                pending=False,
                description='simple rule 2',
                features={},
                expires_at=None,
            ),
            EntityLabelMutation(
                label_name='garply',
                reason_name='RWithFString',
                status=LabelStatus.ADDED,
                pending=False,
                description='fstring rule description with name {UserName}',
                features={'UserName': 'Wumpus'},
                expires_at=None,
            ),
        ],
    }

    label_effects = _get_label_effects_from_result(result)
    assert _compare_effects(actual=label_effects, expected=expected)


def test_when_rules_apply_if(execute_with_result: ExecuteWithResultFunction) -> None:
    now = datetime.now()
    result = execute_with_result(
        {
            'main.sml': """
                RT1 = Rule(when_all=[True], description='rule 1')
                RT2 = Rule(when_all=[True], description='rule 2')
                RFalse = Rule(when_all=[False], description='rule false')
                E1 = Entity(type='MyEntity', id='entity 1')
                E2 = Entity(type='MyEntity', id='entity 2')
                E3 = Entity(type='MyEntity', id='entity 3')
                E4 = Entity(type='MyEntity', id='entity 4')
                E5 = Entity(type='MyEntity', id='entity 5')

                WhenRules(
                    rules_any=[
                        RT1
                    ],
                    then=[
                        LabelAdd(entity=E1, label='foo', apply_if=RT2),
                        LabelAdd(entity=E2, label='bar', apply_if=RFalse),
                        LabelAdd(entity=E3, label='baz'),
                        LabelAdd(entity=E4, label='owo', apply_if=RFalse),
                        LabelAdd(entity=E5, label='uwu', apply_if=RT2),
                    ],
                )
            """,
            'config.yaml': json.dumps(
                {
                    'labels': {
                        label_name: {'valid_for': ['MyEntity']} for label_name in ('foo', 'bar', 'baz', 'owo', 'uwu')
                    }
                }
            ),
        },
        action_time=now,
    )

    expected: Mapping[EntityT[Any], Sequence[EntityLabelMutation]] = {
        EntityT(type='MyEntity', id='entity 1'): [
            EntityLabelMutation(
                label_name='foo',
                reason_name='RT1',
                status=LabelStatus.ADDED,
                pending=False,
                description='rule 1',
                features={},
            ),
            EntityLabelMutation(
                label_name='foo',
                reason_name='RT2',
                status=LabelStatus.ADDED,
                pending=False,
                description='rule 2',
                features={},
            ),
        ],
        EntityT(type='MyEntity', id='entity 3'): [
            EntityLabelMutation(
                label_name='baz',
                reason_name='RT1',
                status=LabelStatus.ADDED,
                pending=False,
                description='rule 1',
                features={},
            ),
        ],
        EntityT(type='MyEntity', id='entity 5'): [
            EntityLabelMutation(
                label_name='uwu',
                reason_name='RT1',
                status=LabelStatus.ADDED,
                pending=False,
                description='rule 1',
                features={},
            ),
            EntityLabelMutation(
                label_name='uwu',
                reason_name='RT2',
                status=LabelStatus.ADDED,
                pending=False,
                description='rule 2',
                features={},
            ),
        ],
    }

    label_effects = _get_label_effects_from_result(result)
    assert _compare_effects(actual=label_effects, expected=expected)


def test_when_rules_apply_if_with_failed_rule(execute_with_result: ExecuteWithResultFunction) -> None:
    now = datetime.now()
    result = execute_with_result(
        {
            'main.sml': """
                R1 = Rule(when_all=[True], description='rule 1')
                R2 = Rule(when_all=[True], description='rule 2')
                RFailed = Rule(when_all=[FailingUdf()], description='rule failed')
                E1 = Entity(type='MyEntity', id='entity 1')

                WhenRules(
                    rules_any=[
                        R1
                    ],
                    then=[
                        LabelAdd(entity=E1, label='foo', apply_if=RFailed),
                        LabelAdd(entity=E1, label='bar'),
                        LabelAdd(entity=E1, label='owo', apply_if=RFailed),
                        LabelAdd(entity=E1, label='uwu', apply_if=R2),
                    ],
                )
            """,
            'config.yaml': json.dumps(
                {'labels': {label_name: {'valid_for': ['MyEntity']} for label_name in ('foo', 'bar', 'owo', 'uwu')}}
            ),
        },
        action_time=now,
    )

    expected: Mapping[EntityT[Any], Sequence[EntityLabelMutation]] = {
        EntityT(type='MyEntity', id='entity 1'): [
            EntityLabelMutation(
                label_name='bar',
                reason_name='R1',
                status=LabelStatus.ADDED,
                pending=False,
                description='rule 1',
                features={},
            ),
            EntityLabelMutation(
                label_name='uwu',
                reason_name='R1',
                status=LabelStatus.ADDED,
                pending=False,
                description='rule 1',
                features={},
            ),
            EntityLabelMutation(
                label_name='uwu',
                reason_name='R2',
                status=LabelStatus.ADDED,
                pending=False,
                description='rule 2',
                features={},
            ),
        ]
    }

    label_effects = _get_label_effects_from_result(result)
    assert _compare_effects(actual=label_effects, expected=expected)


def test_rule_will_error_if_variable_interpolation_attempted_in_non_fstring(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            X = "hello world"
            R1 = Rule(when_all=[True], description='Interpolated: {X}')
            """
        )


def test_rules_must_be_stored_in_variables(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            E = Entity(type='MyEntity', id='hello')
            Rule(when_all=[True], description='')
            WhenRules(
                rules_any=[Rule(when_all=[True], description='')],
                then=[LabelAdd(entity=E, label='foo')],
            )
            _Local = Rule(when_all=[True], description='')
            """
        )


def test_rules_must_have_str_or_f_str_descriptions(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    run_validation('R = Rule(when_all=[True], description="hello")')
    run_validation('R = Rule(when_all=[True], description=f"hello")')
    with check_failure():
        run_validation(
            """
            description = "hey"
            R = Rule(when_all=[True], description=description)
            """
        )


def _sort_entity_mutations(
    effects: Mapping[EntityT[Any], Sequence[EntityLabelMutation]],
) -> Mapping[EntityT[Any], Sequence[EntityLabelMutation]]:
    """Sorts entity mutations so that two sets of effects can be compared easily."""

    def sort_key(mutation: EntityLabelMutation) -> tuple:
        # Create a sorting key from the EntityLabelMutation fields
        # Extract comparable values
        expires_at_key = (mutation.expires_at.timestamp() if mutation.expires_at is not None else 0,)
        features_key = tuple(sorted(mutation.features.items()))

        return (
            mutation.label_name,
            mutation.reason_name,
            mutation.status,
            mutation.pending,
            mutation.description,
            features_key,
            expires_at_key,
        )

    return {entity: sorted(mutations, key=sort_key) for entity, mutations in effects.items()}


def _to_simple_dict(label_effects: Mapping[EntityT[Any], Sequence[EntityLabelMutation]]) -> Dict[object, object]:
    """Converts effects to bare dicts, so py.test can display them better in failure output!"""

    def entity_mutation_to_dict(mutation: EntityLabelMutation) -> Dict[str, Any]:
        # Convert EntityLabelMutation to a comparable dict
        expires_at_timestamp = mutation.expires_at.timestamp() if mutation.expires_at is not None else None

        return {
            'label_name': mutation.label_name,
            'reason_name': mutation.reason_name,
            'status': mutation.status,
            'pending': mutation.pending,
            'description': mutation.description,
            'features': dict(mutation.features),
            'expires_at': expires_at_timestamp,
        }

    return {
        dataclasses.astuple(e): [entity_mutation_to_dict(m) for m in lbls] for e, lbls in sorted(label_effects.items())
    }


def _compare_effects(
    actual: Mapping[EntityT[Any], Sequence[EntityLabelMutation]],
    expected: Mapping[EntityT[Any], Sequence[EntityLabelMutation]],
) -> bool:
    """Given the actual effects from classification, and the expected effects, compare them to make sure they
    are equal."""
    actual = _sort_entity_mutations(actual)
    expected = _sort_entity_mutations(expected)

    # Simple assertion with nice error messages.
    assert _to_simple_dict(actual) == _to_simple_dict(expected)
    # In case the above somehow didn't fail, make sure we still fail if actually different
    assert actual == expected

    return True
