import json
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.validate_experiments import (
    ExperimentValidationResult,
    ValidateExperiments,
    ValidateExperimentsResult,
)
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.experiments import ExperimentT
from osprey.worker.lib.data_exporters.validation_result_exporter import ExperimentValidationResultExporter
from osprey.worker.lib.publisher import PubSubPublisher


@pytest.fixture
def experiment_validation_result_exporter() -> ExperimentValidationResultExporter:
    return ExperimentValidationResultExporter(PubSubPublisher('test_project', 'test_topic'))


def create_experiments() -> List[ExperimentT]:
    user_entity = EntityT(type='User', id='4321')
    guild_entity = EntityT(type='Guild', id='1234')
    return [
        ExperimentT(
            name='Experiment1',
            entity=user_entity,
            buckets=['a', 'b', 'c'],
            bucket_sizes=[10.1, 10.1, 10.1],
            resolved_bucket='b',
            version=1,
            revision=1,
        ),
        ExperimentT(
            name='Experiment2',
            entity=user_entity,
            buckets=['a', 'b'],
            bucket_sizes=[10.1, 10.1],
            resolved_bucket='',
            version=2,
            revision=1,
        ),
        ExperimentT(
            name='Experiment3',
            entity=guild_entity,
            buckets=['a', 'b'],
            bucket_sizes=[10.1, 10.1],
            resolved_bucket='a',
            version=2,
            revision=1,
        ),
    ]


def get_validate_experiments_result(experiments: List[ExperimentT]) -> ValidateExperimentsResult:
    experiment_validation_results = {
        e.name: ExperimentValidationResult(
            name=e.name,
            buckets=e.buckets,
            bucket_sizes=e.bucket_sizes,
            version=e.version,
            revision=e.revision,
            experiment_type=e.entity.type,
        )
        for e in experiments
    }
    return ValidateExperimentsResult(experiment_validation_results=experiment_validation_results)


def get_validated_sources() -> ValidatedSources:
    experiments = create_experiments()
    validate_experiment_results = get_validate_experiments_result(experiments=experiments)
    sources_to_use = {'main.sml': ''}
    return ValidatedSources(
        sources=Sources.from_dict(sources_to_use),
        validation_results={ValidateExperiments: validate_experiment_results},
        warnings=[],
    )


def assert_experiment_metadata_event(experiment: str, experiment_payload: Dict[str, Any]) -> None:
    if experiment == 'Experiment1':
        assert experiment_payload['experiment'] == 'Experiment1'
        assert experiment_payload['buckets'] == ['a', 'b', 'c']
        assert experiment_payload['bucket_sizes_v2'] == [10.1, 10.1, 10.1]
        assert experiment_payload['experiment_version'] == 1
        assert experiment_payload['experiment_revision'] == 1
        assert experiment_payload['experiment_type'] == 'User'
    elif experiment == 'Experiment2':
        assert experiment_payload['experiment'] == 'Experiment2'
        assert experiment_payload['buckets'] == ['a', 'b']
        assert experiment_payload['bucket_sizes_v2'] == [10.1, 10.1]
        assert experiment_payload['experiment_version'] == 2
        assert experiment_payload['experiment_revision'] == 1
        assert experiment_payload['experiment_type'] == 'User'
    elif experiment == 'Experiment3':
        assert experiment_payload['experiment'] == 'Experiment3'
        assert experiment_payload['buckets'] == ['a', 'b']
        assert experiment_payload['bucket_sizes_v2'] == [10.1, 10.1]
        assert experiment_payload['experiment_version'] == 2
        assert experiment_payload['experiment_revision'] == 2
        assert experiment_payload['experiment_type'] == 'Guild'
    else:
        assert False, f"Invalid experiment '{experiment}'"


def test_experiment_validation_result_exporter(
    experiment_validation_result_exporter: ExperimentValidationResultExporter, pubsub_client_mock: MagicMock
) -> None:
    validated_sources = get_validated_sources()
    experiment_validation_result_exporter.send(validated_sources=validated_sources)

    assert len(pubsub_client_mock.publish.call_args_list) == 3

    topic, experiment_metadata_payload_encoded = pubsub_client_mock.publish.call_args_list[0][0]
    experiment_payload = json.loads(experiment_metadata_payload_encoded.decode('utf8'))
    assert topic == 'projects/test_project/topics/test_topic'

    assert_experiment_metadata_event(experiment_payload['experiment'], experiment_payload)

    _, experiment_payload_encoded = pubsub_client_mock.publish.call_args_list[1][0]
    experiment_payload = json.loads(experiment_payload_encoded.decode('utf8'))

    assert_experiment_metadata_event(experiment_payload['experiment'], experiment_payload)


def test_hash_experiment_type() -> None:
    for experiment in create_experiments():
        try:
            hash(experiment)
        except TypeError:
            assert False, 'ExperimentT unhashable.'
