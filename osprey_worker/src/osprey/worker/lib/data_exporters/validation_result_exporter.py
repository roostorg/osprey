import abc

from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.validate_experiments import (
    ValidateExperiments,
)
from osprey.worker.lib.data_exporters.models import ospreyExperimentMetadataUpdate
from osprey.worker.lib.publisher import BasePublisher, PubSubPublisher
from osprey.worker.lib.singletons import CONFIG


class BaseValidationResultExporter(abc.ABC):
    """Base class for sending validation results to external sources"""

    @abc.abstractmethod
    def send(self, validated_sources: ValidatedSources) -> None:
        raise NotImplementedError


class NullValidationResultExporter(BaseValidationResultExporter):
    """Base class for sending validation results to external sources"""

    def send(self, validated_sources: ValidatedSources) -> None:
        return


class ExperimentValidationResultExporter(BaseValidationResultExporter):
    """Sends experiment validation results to the experiment metadata pipeline"""

    def __init__(self, publisher: BasePublisher):
        self._publisher = publisher

    def send(self, validated_sources: ValidatedSources) -> None:
        validate_experiment_results = validated_sources.get_validator_result(ValidateExperiments)
        for experiment in validate_experiment_results.experiments.values():
            experiment_update = ospreyExperimentMetadataUpdate(
                experiment=experiment.name,
                buckets=experiment.buckets,
                bucket_sizes_v2=experiment.bucket_sizes,
                experiment_version=experiment.version,
                experiment_revision=experiment.revision,
                rules_hash=validated_sources.sources.hash(),
                experiment_type=experiment.experiment_type,
            )
            self._publisher.publish(experiment_update)


def get_validation_result_exporter() -> BaseValidationResultExporter:
    """setup and returns the validation result exporter that will run during source updates"""
    config = CONFIG.instance()

    # Use null exporter if disabled (for development)
    if config.get_bool('OSPREY_DISABLE_VALIDATION_EXPORTER', False):
        return NullValidationResultExporter()

    pubsub_project_id = config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev')
    pubsub_topic_id = config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics')
    return ExperimentValidationResultExporter(publisher=PubSubPublisher(pubsub_project_id, pubsub_topic_id))
