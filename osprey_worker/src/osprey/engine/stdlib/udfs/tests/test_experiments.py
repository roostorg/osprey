from typing import Any, Callable, List, Tuple
from unittest import mock

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.language_types.experiments import NOT_IN_EXPERIMENT_BUCKET, NOT_IN_EXPERIMENT_BUCKET_INDEX
from osprey.engine.stdlib.udfs.entity import Entity
from osprey.engine.stdlib.udfs.experiments import (
    CONTROL_BUCKET,
    EXPERIMENT_GRANULARITY,
    Experiment,
    ExperimentWhen,
)
from osprey.engine.stdlib.udfs.rules import Rule
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(Entity, Rule, Experiment, ExperimentWhen)),
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
]


@pytest.mark.parametrize(
    'experiment_name, entity_id',
    (('E1', 'ID1'), ('E2', 'ID2'), ('E3', 'ID3'), ('E4', 'ID3')),
)
def test_hash_mod_should_mod(experiment_name: str, entity_id: str) -> None:
    hash_mod_value = Experiment.hash_mod(experiment_name, entity_id)
    assert hash_mod_value >= 0 and hash_mod_value < EXPERIMENT_GRANULARITY


@pytest.mark.parametrize(
    'hash_keys',
    [(('E1', 'ID1'), ('E1', 'ID1'), ('E1', 'ID1'))],
)
def test_consistent_hash_mod(hash_keys: List[Tuple[str, str]]) -> None:
    hash_mod_values = []
    for experiment_name, entity_id in hash_keys:
        hash_mod_values.append(Experiment.hash_mod(experiment_name, entity_id))
    assert hash_mod_values[0] == 8548
    assert len(set(hash_mod_values)) == 1


def test_experiment_bucketing(execute: ExecuteFunction) -> None:
    experiment = f"""
    E1 = Entity(type='MyEntity', id='entity 1')
    A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'treatment'], bucket_sizes=[50.0, 50.0], version=1, revision=1)
    """
    data = execute(experiment)
    assert data['A'] == [
        'A',
        'entity 1',
        'MyEntity',
        CONTROL_BUCKET,
        str(0),
        str(1),
        str(1),
        str(True),
    ]


def test_experiment_same_number_of_buckets_and_sizes(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            E1 = Entity(type='MyEntity', id='entity 1')
            A = Experiment(entity=E1, buckets=['control', 'b', 'c'], bucket_sizes=[15, 20], version=1, revision=1)
            """
        )


def test_experiment_bucket_size_precision_too_high(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            f"""
            E1 = Entity(type='MyEntity', id='entity 1')
            A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'b'], bucket_sizes=[10, 2.512], version=2, revision=0)
            """
        )


def test_experiment_version_error(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation(
            f"""
            E1 = Entity(type='MyEntity', id='entity 1')
            A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'b'], bucket_sizes=[10, 10], version=-1, revision=1)
            """
        )


def test_experiment_revision_error(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation(
            f"""
            E1 = Entity(type='MyEntity', id='entity 1')
            A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'b'], bucket_sizes=[10, 10], version=1, revision=-1)
            """
        )


@pytest.mark.parametrize(
    'buckets, bucket_sizes, mock_hash_value, expected_bucket, expected_bucket_index',
    (
        ([CONTROL_BUCKET, 'b'], [2.5, 2.5], 0, CONTROL_BUCKET, 0),
        ([CONTROL_BUCKET, 'b'], [2.5, 2.5], 250, NOT_IN_EXPERIMENT_BUCKET, NOT_IN_EXPERIMENT_BUCKET_INDEX),
        ([CONTROL_BUCKET, 'b'], [2.5, 2.5], 5000, 'b', 1),
        ([CONTROL_BUCKET, 'b'], [2.5, 2.5], 9999, NOT_IN_EXPERIMENT_BUCKET, NOT_IN_EXPERIMENT_BUCKET_INDEX),
        ([CONTROL_BUCKET, 'b', 'c'], [5.55, 10.0, 20.0], 0, CONTROL_BUCKET, 0),
        ([CONTROL_BUCKET, 'b', 'c'], [5.55, 10.0, 20.0], 554, CONTROL_BUCKET, 0),
        ([CONTROL_BUCKET, 'b', 'c'], [5.55, 10.0, 20.0], 555, NOT_IN_EXPERIMENT_BUCKET, NOT_IN_EXPERIMENT_BUCKET_INDEX),
        (
            [CONTROL_BUCKET, 'b', 'c'],
            [5.55, 10.0, 20.0],
            3332,
            NOT_IN_EXPERIMENT_BUCKET,
            NOT_IN_EXPERIMENT_BUCKET_INDEX,
        ),
        ([CONTROL_BUCKET, 'b', 'c'], [5.55, 10.0, 20.0], 3333, 'b', 1),
        ([CONTROL_BUCKET, 'b', 'c'], [5.55, 10.0, 20.0], 4332, 'b', 1),
        (
            [CONTROL_BUCKET, 'b', 'c'],
            [5.55, 10.0, 20.0],
            4333,
            NOT_IN_EXPERIMENT_BUCKET,
            NOT_IN_EXPERIMENT_BUCKET_INDEX,
        ),
        (
            [CONTROL_BUCKET, 'b', 'c'],
            [5.55, 10.0, 20.0],
            6665,
            NOT_IN_EXPERIMENT_BUCKET,
            NOT_IN_EXPERIMENT_BUCKET_INDEX,
        ),
        ([CONTROL_BUCKET, 'b', 'c'], [5.55, 10.0, 20.0], 6666, 'c', 2),
        ([CONTROL_BUCKET, 'b', 'c'], [5.55, 10.0, 20.0], 8665, 'c', 2),
        (
            [CONTROL_BUCKET, 'b', 'c'],
            [5.55, 10.0, 20.0],
            8666,
            NOT_IN_EXPERIMENT_BUCKET,
            NOT_IN_EXPERIMENT_BUCKET_INDEX,
        ),
        (
            [CONTROL_BUCKET, 'b', 'c'],
            [33.33, 33.33, 33.33],
            9999,
            NOT_IN_EXPERIMENT_BUCKET,
            NOT_IN_EXPERIMENT_BUCKET_INDEX,
        ),
    ),
)
@mock.patch.object(Experiment, 'hash_mod')
def test_experiment_resolution(
    hash_mod_mock: mock.MagicMock,
    execute: ExecuteFunction,
    buckets: List[str],
    bucket_sizes: List[float],
    mock_hash_value: int,
    expected_bucket: str,
    expected_bucket_index: int,
) -> None:
    hash_mod_mock.return_value = mock_hash_value
    experiment = f"""
    E1 = Entity(type='MyEntity', id='entity 1')
    A = Experiment(entity=E1, buckets={str(buckets)}, bucket_sizes={str(bucket_sizes)}, version=1, revision=1)
    """
    data = execute(experiment)
    assert data['A'] == [
        'A',
        'entity 1',
        'MyEntity',
        expected_bucket,
        str(expected_bucket_index),
        str(1),
        str(1),
        str(True),
    ]


def test_experimentwhen_uses_valid_experiment(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            f"""
            E1 = Entity(type='MyEntity', id='entity 1')
            A = 'blah'
            EW = ExperimentWhen({CONTROL_BUCKET}=[True, True], branch_b=[False, False], experiment=A)
            """
        )


def test_experimentwhen_too_many_buckets(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            f"""
            E1 = Entity(type='MyEntity', id='entity 1')
            A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'b'], bucket_sizes=[5, 5], version=1, revision=1)
            EW = ExperimentWhen({CONTROL_BUCKET}=[True, True, True], b=[True, True], c=[True], experiment=A)
            R = Rule(when_all=[EW], description='')
            """
        )


def test_experimentwhen_too_few_buckets(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            f"""
            E1 = Entity(type='MyEntity', id='entity 1')
            A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'b'], bucket_sizes=[5, 5], version=1, revision=1)
            EW = ExperimentWhen({CONTROL_BUCKET}=[True, True, True], experiment=A)
            R = Rule(when_all=EW, description='')
            """
        )


@mock.patch.object(Experiment, 'hash_mod')
def test_experimentwhen_use_control_branch_when_not_in_experiment(
    hash_mod_mock: mock.MagicMock, execute: ExecuteFunction
) -> None:
    hash_mod_mock.return_value = 9999
    experiment = f"""
    E1 = Entity(type='MyEntity', id='entity 1')
    A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'b', 'c'], bucket_sizes=[33.3, 33.3, 33.3], version=1, revision=1)
    B = ExperimentWhen({CONTROL_BUCKET}=[True, True, True], b=[True, False, True], c=[False], experiment=A)
    """
    data = execute(experiment)
    assert data['B'] == [True, True, True]


@pytest.mark.parametrize(
    'mock_hash_value, expected_value',
    ((0, [True, True, True]), (3334, [False, False]), (6668, [False, True]), (9999, [True, True, True])),
)
@mock.patch.object(Experiment, 'hash_mod')
def test_experimentwhen_results(
    hash_mod_mock: mock.MagicMock,
    execute: ExecuteFunction,
    mock_hash_value: int,
    expected_value: bool,
) -> None:
    hash_mod_mock.return_value = mock_hash_value
    experiment = f"""
    E1 = Entity(type='MyEntity', id='entity 1')
    A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'b', 'c'], bucket_sizes=[10.1, 10.1, 10.1], version=1, revision=1)
    B = ExperimentWhen({CONTROL_BUCKET}=[True, True, True], b=[False, False], c=[False, True], experiment=A)
    """
    data = execute(experiment)
    assert data['B'] == expected_value


@mock.patch.object(Experiment, 'hash_mod')
def test_inline_experimentwhen(
    hash_mod_mock: mock.MagicMock,
    execute: ExecuteFunction,
) -> None:
    hash_mod_mock.return_value = 3500
    experiment = f"""
    E1 = Entity(type='MyEntity', id='entity 1')
    A = Experiment(entity=E1, buckets=['{CONTROL_BUCKET}', 'b', 'c'],bucket_sizes=[5.0, 5.0, 5.0], version=1, revision=1)
    R = Rule(when_all=ExperimentWhen({CONTROL_BUCKET}=[True, False], b=[True], c=[False], experiment=A), description='')
    """
    data = execute(experiment)
    assert data['R'] is True
