from dataclasses import dataclass
from functools import lru_cache
from typing import cast

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.base_validator import BaseValidator, HasInput, HasResult

from ..validation_context import ValidationContext
from .feature_name_to_entity_type_mapping import FeatureNameToEntityTypeMapping
from .validate_call_kwargs import ValidateCallKwargs


@dataclass
class ExperimentValidationResult:
    name: str
    buckets: list[str]
    bucket_sizes: list[float]
    version: int
    revision: int
    experiment_type: str


class ValidateExperimentsResult:
    def __init__(self, experiment_validation_results: dict[str, ExperimentValidationResult]):
        self._experiments = experiment_validation_results

    def get_experiment(self, experiment_name: str) -> ExperimentValidationResult | None:
        return self._experiments.get(experiment_name)

    @property
    def experiments(self) -> dict[str, ExperimentValidationResult]:
        return self._experiments


# NEED FOR MVP (not really but no harm !)


class ValidateExperiments(BaseValidator, HasInput[dict[str, grammar.Call]], HasResult[ValidateExperimentsResult]):
    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)

    def run(self) -> None:
        # all the needed validation is done in the experiment UDFs which is called from ValidateCallKwargs
        self.context.validator_depends_on(validator_classes=[ValidateCallKwargs, FeatureNameToEntityTypeMapping])
        self._experiment_nodes: dict[str, grammar.Call] = self.context.get_validator_input(type(self), {})

    @lru_cache(maxsize=1)
    def get_result(self) -> ValidateExperimentsResult:
        return ValidateExperimentsResult(experiment_validation_results=self._get_validation_results())

    def get_entity_type(self, name: str) -> str:
        return self.context.get_validator_result(FeatureNameToEntityTypeMapping)[name]

    def _get_validation_results(self) -> dict[str, ExperimentValidationResult]:
        return {
            k: ExperimentValidationResult(
                name=k,
                buckets=self._unwrap_string_list(cast(grammar.List, v.argument_dict()['buckets'])),
                bucket_sizes=self._unwrap_float_list(cast(grammar.List, v.argument_dict()['bucket_sizes'])),
                version=cast(int, cast(grammar.Number, v.argument_dict()['version']).value),
                revision=cast(int, cast(grammar.Number, v.argument_dict()['revision']).value),
                experiment_type=self.get_entity_type(cast(grammar.Name, v.argument_dict()['entity']).identifier),
            )
            for k, v in self._experiment_nodes.items()
        }

    @staticmethod
    def _unwrap_string_list(string_list: grammar.List) -> list[str]:
        return [s.value for s in cast(list[grammar.String], string_list.items)]

    @staticmethod
    def _unwrap_float_list(string_list: grammar.List) -> list[float]:
        return [s.value for s in cast(list[grammar.Number], string_list.items)]
