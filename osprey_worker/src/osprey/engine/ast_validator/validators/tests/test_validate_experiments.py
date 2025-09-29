from typing import cast

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_experiments import (
    ExperimentValidationResult,
    ValidateExperiments,
    ValidateExperimentsResult,
)
from osprey.engine.conftest import ExecuteWithResultFunction
from osprey.engine.stdlib.udfs.experiments import CONTROL_BUCKET

pytestmark = [
    pytest.mark.use_validators(
        [
            ValidateExperiments,
            # Dependencies of ValidateRules
            ValidateCallKwargs,
            UniqueStoredNames,
        ]
    ),
    pytest.mark.use_osprey_stdlib,
]


def test_experiment_with_result_json(execute_with_result: ExecuteWithResultFunction) -> None:
    experiment = f"""
    E1 = Entity(type='MyEntity', id='entity 1')
    A = Experiment(
        entity=E1, buckets=['{CONTROL_BUCKET}', 'b'], bucket_sizes=[2.5, 2.5], version=1,
        revision=1, local_bucketing=True
    )
    """
    data = execute_with_result(experiment)
    assert data.extracted_features_json


def test_validate_experiment_result(execute_with_result: ExecuteWithResultFunction) -> None:
    experiment = f"""
    E1 = Entity(type='MyEntity', id='entity 1')
    A = Experiment(
        entity=E1, buckets=['{CONTROL_BUCKET}', 'b'], bucket_sizes=[2.5, 2.5], version=1,
        revision=1, local_bucketing=True
    )
    """
    data = execute_with_result(experiment)
    validate_experiment_result = cast(ValidateExperimentsResult, data.validator_results[ValidateExperiments])
    assert validate_experiment_result.get_experiment('A') == ExperimentValidationResult(
        'A', [CONTROL_BUCKET, 'b'], [2.5, 2.5], version=1, revision=1, experiment_type='MyEntity'
    )
