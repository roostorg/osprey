import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

import gevent
import pytest
from gevent.event import Event
from gevent.pool import Pool
from osprey.engine.ast_validator.validators.feature_name_to_entity_type_mapping import (
    FeatureNameToEntityTypeMapping,
)
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_labels import ValidateLabels
from osprey.engine.conftest import (
    CheckFailureFunction,
    ExecuteFunction,
    ExecuteWithResultFunction,
    RunValidationFunction,
)
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelStatus
from osprey.engine.stdlib import get_config_registry
from osprey.engine.stdlib.udfs.entity import Entity
from osprey.engine.stdlib.udfs.labels import EmptyEntityError, HasLabel, LabelAdd, LabelRemove
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.stdlib.udfs.time_delta import TimeDelta
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.osprey_shared.labels import (
    EntityLabelMutation,
    EntityLabelMutationsResult,
    EntityLabels,
    LabelReason,
    LabelReasons,
    LabelState,
)
from osprey.worker.lib.storage.labels import LabelsProvider
from result import Result

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators(
        [
            ValidateLabels,
            FeatureNameToEntityTypeMapping,
            ValidateCallKwargs,
            UniqueStoredNames,
            get_config_registry().get_validator(),
        ]
    ),
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(LabelAdd, LabelRemove, HasLabel, Entity, Rule, WhenRules, TimeDelta)
    ),
]


class StaticLabelProvider(LabelsProvider):
    def __init__(self, entity_labels: Dict[EntityT[Any], EntityLabels]) -> None:
        self._entity_labels = entity_labels

    def get_from_service(self, key: EntityT[Any]) -> EntityLabels:
        return self._entity_labels[key]

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[EntityLabels, Exception]]:
        return [Result.Ok(self.get_from_service(key)) for key in keys]

    def apply_entity_mutation(
        self, entity_key: EntityT[Any], mutations: List[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        return self.apply_entity_label_mutations(entity_key, mutations)


class BlockingLabelProvider(StaticLabelProvider):
    def __init__(self, entity_labels: Dict[EntityT[Any], EntityLabels]) -> None:
        super().__init__(entity_labels)
        self.blocking_events: List[Event] = []
        self.calls: List[EntityT[Any]] = []

    def get_from_service(self, key: EntityT[Any]) -> EntityLabels:
        event = Event()
        self.blocking_events.append(event)
        event.wait()
        self.blocking_events.remove(event)

        self.calls.append(key)
        return super().get_from_service(key)


def source_with_labels_config(source: str, labels: Set[str]) -> Dict[str, str]:
    config = json.dumps({'labels': {label: {} for label in labels}})
    return {'main.sml': source, 'config.yaml': config}


@pytest.mark.parametrize(
    'checking_status, manual, actual_status, reasons, result',
    (
        ('added', None, LabelStatus.ADDED, LabelReasons({'TestReason': LabelReason()}), True),
        (
            'added',
            None,
            LabelStatus.ADDED,
            LabelReasons({'ExpiredReason': LabelReason(expires_at=(datetime.now(timezone.utc) - timedelta(hours=1)))}),
            False,
        ),
        (
            'added',
            None,
            LabelStatus.ADDED,
            LabelReasons(
                {
                    'ExpiredReason': LabelReason(expires_at=(datetime.now(timezone.utc) - timedelta(hours=1))),
                    'TestReason': LabelReason(),
                }
            ),
            True,
        ),
        (
            'added',
            None,
            LabelStatus.ADDED,
            LabelReasons(
                {
                    'ExpiredReason': LabelReason(expires_at=(datetime.now(timezone.utc) - timedelta(hours=1))),
                    'ExpiringReason': LabelReason(expires_at=(datetime.now(timezone.utc) + timedelta(hours=1))),
                }
            ),
            True,
        ),
        (
            'added',
            None,
            LabelStatus.ADDED,
            LabelReasons({'ExpiringReason': LabelReason(expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)))}),
            True,
        ),
        ('added', None, LabelStatus.MANUALLY_ADDED, LabelReasons({'TestReason': LabelReason()}), True),
        ('added', None, LabelStatus.REMOVED, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', None, LabelStatus.MANUALLY_REMOVED, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', None, None, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', True, LabelStatus.ADDED, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', True, LabelStatus.MANUALLY_ADDED, LabelReasons({'TestReason': LabelReason()}), True),
        ('added', True, LabelStatus.REMOVED, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', True, LabelStatus.MANUALLY_REMOVED, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', True, None, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', False, LabelStatus.ADDED, LabelReasons({'TestReason': LabelReason()}), True),
        ('added', False, LabelStatus.MANUALLY_ADDED, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', False, LabelStatus.REMOVED, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', False, LabelStatus.MANUALLY_REMOVED, LabelReasons({'TestReason': LabelReason()}), False),
        ('added', False, None, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', None, LabelStatus.ADDED, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', None, LabelStatus.MANUALLY_ADDED, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', None, LabelStatus.REMOVED, LabelReasons({'TestReason': LabelReason()}), True),
        ('removed', None, LabelStatus.MANUALLY_REMOVED, LabelReasons({'TestReason': LabelReason()}), True),
        ('removed', None, None, LabelReasons({'TestReason': LabelReason()}), True),
        ('removed', True, LabelStatus.ADDED, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', True, LabelStatus.MANUALLY_ADDED, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', True, LabelStatus.REMOVED, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', True, LabelStatus.MANUALLY_REMOVED, LabelReasons({'TestReason': LabelReason()}), True),
        ('removed', True, None, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', False, LabelStatus.ADDED, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', False, LabelStatus.MANUALLY_ADDED, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', False, LabelStatus.REMOVED, LabelReasons({'TestReason': LabelReason()}), True),
        ('removed', False, LabelStatus.MANUALLY_REMOVED, LabelReasons({'TestReason': LabelReason()}), False),
        ('removed', False, None, LabelReasons({'TestReason': LabelReason()}), True),
    ),
)
def test_get_labels_retrieves_data(
    execute: ExecuteFunction,
    checking_status: str,
    manual: Optional[bool],
    actual_status: Optional[LabelStatus],
    reasons: LabelReasons,
    result: bool,
) -> None:
    if actual_status is None:
        labels = EntityLabels(labels={})
    else:
        labels = EntityLabels(labels={'my_label': LabelState(status=actual_status, reasons=reasons)})
    label_provider = StaticLabelProvider({EntityT('MyEntity', 'my_id'): labels})
    data = execute(
        source_with_labels_config(
            f"""
            L = HasLabel(
                entity=Entity(type='MyEntity', id='my_id'),
                label='my_label',
                status='{checking_status}',
                manual={manual},
            )
            """,
            labels={'my_label'},
        ),
        udf_helpers=UDFHelpers().set_udf_helper(HasLabel, label_provider),
    )
    assert data == {'L': result}


@pytest.mark.parametrize(
    'checking_status, manual, actual_status, reasons, min_label_age, result',
    (
        (
            'added',
            None,
            LabelStatus.ADDED,
            LabelReasons({'TestReason': LabelReason(created_at=(datetime.now(timezone.utc) - timedelta(days=1)))}),
            timedelta(days=1),
            True,
        ),
        (
            'added',
            None,
            LabelStatus.ADDED,
            LabelReasons({'TestReason': LabelReason(created_at=(datetime.now(timezone.utc)))}),
            timedelta(days=1),
            False,
        ),
    ),
)
def test_get_labels_retrieves_data_added_after(
    execute: ExecuteFunction,
    checking_status: str,
    manual: Optional[bool],
    actual_status: Optional[LabelStatus],
    reasons: LabelReasons,
    min_label_age: timedelta,
    result: bool,
) -> None:
    if actual_status is None:
        labels = EntityLabels(labels={})
    else:
        labels = EntityLabels(labels={'my_label': LabelState(status=actual_status, reasons=reasons)})
    label_provider = StaticLabelProvider({EntityT('MyEntity', 'my_id'): labels})
    data = execute(
        source_with_labels_config(
            f"""
            L = HasLabel(
                entity=Entity(type='MyEntity', id='my_id'),
                label='my_label',
                status='{checking_status}',
                manual={manual},
                min_label_age=TimeDelta(days={min_label_age.days}),
            )
            """,
            labels={'my_label'},
        ),
        udf_helpers=UDFHelpers().set_udf_helper(HasLabel, label_provider),
    )
    assert data == {'L': result}


# def test_gets_labels_in_parallel_with_debouncing_and_caching(execute: ExecuteFunction) -> None:
#     label_provider = BlockingLabelProvider(
#         {
#             EntityT('User', 123): Labels(
#                 labels={'awesome': LabelState(status=LabelStatus.ADDED, reasons={'TestReason': LabelReason()})}
#             ),
#             EntityT('Email', 'hello@example.com'): Labels(labels={}),
#         }
#     )
#     # A more limited set of tests that mimic the ExternalServiceAccessor tests
#     main = gevent.spawn(
#         execute,
#         source_with_labels_config(
#             """
#             UserId = Entity(type="User", id=123)
#             UserEmail = Entity(type="Email", id="hello@example.com")
#             L1 = HasLabel(entity=UserId, label="awesome", status="added")
#             L2 = HasLabel(entity=UserEmail, label="neat", status="added")
#             L3 = HasLabel(entity=UserId, label="cool", status="removed")
#             """,
#             labels={'awesome', 'neat', 'cool'},
#         ),
#         udf_helpers=UDFHelpers().set_udf_helper(HasLabel, label_provider),
#         async_pool=Pool(size=10),
#     )
#     gevent.idle()

#     assert len(label_provider.blocking_events) == 2
#     label_provider.blocking_events[0].set()
#     label_provider.blocking_events[1].set()
#     gevent.idle()

#     main.get()

#     assert set(label_provider.calls) == {EntityT('User', 123), EntityT('Email', 'hello@example.com')}


def test_gets_only_debounces_single_execution(execute: ExecuteFunction) -> None:
    label_provider = BlockingLabelProvider({EntityT('User', 123): EntityLabels(labels={})})

    def do_execute() -> None:
        execute(
            source_with_labels_config(
                'L = HasLabel(entity=Entity(type="User", id=123), label="awesome", status="added")',
                labels={'awesome'},
            ),
            udf_helpers=UDFHelpers().set_udf_helper(HasLabel, label_provider),
            async_pool=Pool(size=10),
        )

    g1 = gevent.spawn(do_execute)
    g2 = gevent.spawn(do_execute)
    gevent.idle()

    assert len(label_provider.blocking_events) == 2
    label_provider.blocking_events[0].set()
    label_provider.blocking_events[1].set()
    gevent.idle()

    g1.get()
    g2.get()

    assert label_provider.calls == [EntityT('User', 123), EntityT('User', 123)]


def test_checks_for_valid_status(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation(
            source_with_labels_config(
                """
                E = Entity(type='MyEntity', id='my_id')
                L1 = HasLabel(entity=E, label='my_label', status='manually_added')
                L2 = HasLabel(entity=E, label='my_label', status='manually_removed')
                L3 = HasLabel(entity=E, label='my_label', status='gibberish_status')
                L4 = HasLabel(entity=E, label='my_label', status='ADDED')
                """,
                labels={'my_label'},
            )
        )


def test_checks_for_valid_label(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation(
            source_with_labels_config(
                """
                E = Entity(type='MyEntity', id='my_id')
                L1 = HasLabel(entity=E, label='my_label', status='added')
                L2 = HasLabel(entity=E, label='other_label', status='added')
                # Small typo
                L3 = HasLabel(entity=E, label='my_lable', status='added')
                # Gibberish
                L4 = HasLabel(entity=E, label='not_a_label_aoeuaouaoeu', status='added')
                """,
                labels={'my_label', 'other_label'},
            )
        )


def test_validates_labels_are_valid_for_entity(
    check_failure: CheckFailureFunction, run_validation: RunValidationFunction
) -> None:
    with check_failure():
        run_validation(
            {
                'main.sml': """
                    R = Rule(when_all=[True], description='simple rule')
                    E1 = Entity(type='TypeA', id='entity 1')
                    E2 = Entity(type='TypeB', id='entity 2')
                    WhenRules(
                        rules_any=[R],
                        then=[
                            LabelAdd(entity=E1, label='for_both'),
                            LabelRemove(entity=E1, label='for_both'),
                            LabelAdd(entity=E2, label='for_both'),
                            LabelRemove(entity=E2, label='for_both'),
                            LabelAdd(entity=E1, label='for_a'),
                            LabelRemove(entity=E1, label='for_a'),
                            LabelAdd(entity=E2, label='for_a'),
                            LabelRemove(entity=E2, label='for_a'),
                            LabelAdd(entity=E1, label='for_b'),
                            LabelRemove(entity=E1, label='for_b'),
                            LabelAdd(entity=E2, label='for_b'),
                            LabelRemove(entity=E2, label='for_b'),
                            LabelAdd(entity=E1, label='fake_label'),
                            LabelRemove(entity=E1, label='fake_label'),
                            LabelAdd(entity=E2, label='fake_label'),
                            LabelRemove(entity=E2, label='fake_label'),
                        ],
                    )
                """,
                'config.yaml': json.dumps(
                    {
                        'labels': {
                            'for_a': {'valid_for': ['TypeA', 'Other']},
                            'for_b': {'valid_for': ['TypeB', 'Other']},
                            'for_both': {'valid_for': ['TypeA', 'TypeB']},
                        }
                    }
                ),
            }
        )


def test_suggests_similar_labels(check_failure: CheckFailureFunction, run_validation: RunValidationFunction) -> None:
    with check_failure():
        run_validation(
            {
                'main.sml': """
                    R = Rule(when_all=[True], description='simple rule')
                    E = Entity(type='MyEntity', id='entity 1')
                    WhenRules(
                        rules_any=[R],
                        then=[
                            LabelAdd(entity=E, label='my_lable'),
                        ],
                    )
                """,
                'config.yaml': json.dumps({'labels': {'my_label': {'valid_for': ['MyEntity']}}}),
            }
        )


@pytest.mark.parametrize(
    'entity_type, label_name, label_udf, entity_label_mutation',
    (
        ('EntityA', 'label_a', 'LabelAdd', f'EntityA/label_a/{LabelStatus.ADDED.value}'),
        ('EntityB', 'label_b', 'LabelRemove', f'EntityB/label_b/{LabelStatus.REMOVED.value}'),
    ),
)
def test_label_effects_are_exported_to_extracted_features(
    execute_with_result: ExecuteWithResultFunction,
    entity_type: str,
    label_name: str,
    label_udf: str,
    entity_label_mutation: str,
) -> None:
    result = execute_with_result(
        {
            'main.sml': f"""
                R = Rule(when_all=[True], description='simple rule')
                UserId: Entity[int] = Entity(type='{entity_type}', id=1)
                WhenRules(
                    rules_any=[R],
                    then=[
                        {label_udf}(entity=UserId, label='{label_name}'),
                    ],
                )
            """,
            'config.yaml': json.dumps({'labels': {label_name: {'valid_for': [entity_type]}}}),
        }
    )
    assert set(result.extracted_features['__entity_label_mutations']) == {entity_label_mutation}


@pytest.mark.parametrize(
    'entity_type, label_name, label_udf, entity_label_mutation',
    (
        ('EntityA', 'label_a', 'LabelAdd', f'EntityA/label_a/{LabelStatus.ADDED.value}'),
        ('EntityB', 'label_b', 'LabelRemove', f'EntityB/label_b/{LabelStatus.REMOVED.value}'),
    ),
)
def test_label_effects_are_exported_to_extracted_features_multi_rule(
    execute_with_result: ExecuteWithResultFunction,
    entity_type: str,
    label_name: str,
    label_udf: str,
    entity_label_mutation: str,
) -> None:
    result = execute_with_result(
        {
            'main.sml': f"""
                R = Rule(when_all=[True], description='simple rule')
                R2 = Rule(when_all=[True], description='simple rule')
                UserId: Entity[int] = Entity(type='{entity_type}', id=1)
                WhenRules(
                    rules_any=[R],
                    then=[
                        {label_udf}(entity=UserId, label='{label_name}'),
                    ],
                )
                WhenRules(
                    rules_any=[R2],
                    then=[
                        {label_udf}(entity=UserId, label='{label_name}'),
                    ],
                )
            """,
            'config.yaml': json.dumps({'labels': {label_name: {'valid_for': [entity_type]}}}),
        }
    )
    assert set(result.extracted_features['__entity_label_mutations']) == set([entity_label_mutation])


@pytest.mark.parametrize(
    'entity_type, label_name, entity_label_mutation',
    (
        (
            'EntityA',
            'label_a',
            [f'EntityA/label_a/{LabelStatus.ADDED.value}', f'EntityA/label_a/{LabelStatus.REMOVED.value}'],
        ),
        (
            'EntityB',
            'label_b',
            [f'EntityB/label_b/{LabelStatus.ADDED.value}', f'EntityB/label_b/{LabelStatus.REMOVED.value}'],
        ),
    ),
)
def test_label_effects_are_exported_to_extracted_features_multi_rule_add_and_remove(
    execute_with_result: ExecuteWithResultFunction,
    entity_type: str,
    label_name: str,
    entity_label_mutation: List[str],
) -> None:
    result = execute_with_result(
        {
            'main.sml': f"""
                R = Rule(when_all=[True], description='simple rule')
                R2 = Rule(when_all=[True], description='simple rule')
                UserId: Entity[int] = Entity(type='{entity_type}', id=1)
                WhenRules(
                    rules_any=[R],
                    then=[
                        LabelAdd(entity=UserId, label='{label_name}'),
                    ],
                )
                WhenRules(
                    rules_any=[R2],
                    then=[
                        LabelRemove(entity=UserId, label='{label_name}'),
                    ],
                )
            """,
            'config.yaml': json.dumps({'labels': {label_name: {'valid_for': [entity_type]}}}),
        }
    )
    assert set(result.extracted_features['__entity_label_mutations']) == set(entity_label_mutation)


@pytest.mark.parametrize(
    'entity_type, label_name, entity_label_mutation',
    (
        (
            'EntityA',
            'label_a',
            [f'EntityA/label_a/{LabelStatus.ADDED.value}', f'EntityA/other_label/{LabelStatus.ADDED.value}'],
        ),
        (
            'EntityB',
            'label_b',
            [f'EntityB/label_b/{LabelStatus.ADDED.value}', f'EntityB/other_label/{LabelStatus.ADDED.value}'],
        ),
    ),
)
def test_label_effects_are_exported_to_extracted_features_multi_add(
    execute_with_result: ExecuteWithResultFunction,
    entity_type: str,
    label_name: str,
    entity_label_mutation: List[str],
) -> None:
    result = execute_with_result(
        {
            'main.sml': f"""
                R = Rule(when_all=[True], description='simple rule')
                R2 = Rule(when_all=[True], description='simple rule')
                UserId: Entity[int] = Entity(type='{entity_type}', id=1)
                WhenRules(
                    rules_any=[R],
                    then=[
                        LabelAdd(entity=UserId, label='{label_name}'),
                    ],
                )
                WhenRules(
                    rules_any=[R2],
                    then=[
                        LabelAdd(entity=UserId, label='other_label'),
                    ],
                )
            """,
            'config.yaml': json.dumps(
                {'labels': {label_name: {'valid_for': [entity_type]}, 'other_label': {'valid_for': [entity_type]}}}
            ),
        }
    )
    assert set(result.extracted_features['__entity_label_mutations']) == set(entity_label_mutation)


def test_error_on_empty_raises_when_entity_has_no_labels(execute_with_result: ExecuteWithResultFunction) -> None:
    """error_on_empty=True should raise EmptyEntityError when entity has no labels at all."""
    empty_labels = EntityLabels(labels={})
    label_provider = StaticLabelProvider({EntityT('MyEntity', 'my_id'): empty_labels})

    result = execute_with_result(
        source_with_labels_config(
            """
            L = HasLabel(
                entity=Entity(type='MyEntity', id='my_id'),
                label='my_label',
                error_on_empty=True,
            )
            """,
            labels={'my_label'},
        ),
        udf_helpers=UDFHelpers().set_udf_helper(HasLabel, label_provider),
    )

    assert len(result.error_infos) > 0
    error = result.error_infos[0].error
    assert isinstance(error, EmptyEntityError)
    assert 'MyEntity/my_id' in str(error)
    assert 'my_label' in str(error)


def test_error_on_empty_returns_true_when_label_exists(execute: ExecuteFunction) -> None:
    """error_on_empty=True should return True when the entity has labels and the requested label exists."""
    labels = EntityLabels(
        labels={'my_label': LabelState(status=LabelStatus.ADDED, reasons=LabelReasons({'TestReason': LabelReason()}))}
    )
    label_provider = StaticLabelProvider({EntityT('MyEntity', 'my_id'): labels})

    data = execute(
        source_with_labels_config(
            """
            L = HasLabel(
                entity=Entity(type='MyEntity', id='my_id'),
                label='my_label',
                error_on_empty=True,
            )
            """,
            labels={'my_label'},
        ),
        udf_helpers=UDFHelpers().set_udf_helper(HasLabel, label_provider),
    )

    assert data == {'L': True}


def test_error_on_empty_returns_false_when_label_missing_but_entity_has_other_labels(
    execute: ExecuteFunction,
) -> None:
    """error_on_empty=True should return False when entity has labels but not the requested one."""
    labels = EntityLabels(
        labels={
            'other_label': LabelState(status=LabelStatus.ADDED, reasons=LabelReasons({'TestReason': LabelReason()}))
        }
    )
    label_provider = StaticLabelProvider({EntityT('MyEntity', 'my_id'): labels})

    data = execute(
        source_with_labels_config(
            """
            L = HasLabel(
                entity=Entity(type='MyEntity', id='my_id'),
                label='my_label',
                error_on_empty=True,
            )
            """,
            labels={'my_label', 'other_label'},
        ),
        udf_helpers=UDFHelpers().set_udf_helper(HasLabel, label_provider),
    )

    assert data == {'L': False}


def test_error_on_empty_false_returns_false_when_entity_has_no_labels(execute: ExecuteFunction) -> None:
    """error_on_empty=False (default) should return False when entity has no labels."""
    empty_labels = EntityLabels(labels={})
    label_provider = StaticLabelProvider({EntityT('MyEntity', 'my_id'): empty_labels})

    data = execute(
        source_with_labels_config(
            """
            L = HasLabel(
                entity=Entity(type='MyEntity', id='my_id'),
                label='my_label',
                error_on_empty=False,
            )
            """,
            labels={'my_label'},
        ),
        udf_helpers=UDFHelpers().set_udf_helper(HasLabel, label_provider),
    )

    assert data == {'L': False}
