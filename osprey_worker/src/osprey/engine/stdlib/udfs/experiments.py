from __future__ import annotations

from decimal import Decimal
from enum import Enum
from math import floor
from typing import Any, Optional, Tuple, Type

import mmh3  # type: ignore
from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_utils import add_must_assign_to_variable_error
from osprey.engine.ast_validator.validators.validate_experiments import ValidateExperiments
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.experiments import NOT_IN_EXPERIMENT_BUCKET, ExperimentT
from osprey.engine.stdlib.udfs._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase, ValidationContext
from osprey.engine.stdlib.udfs.categories import UdfCategories

EXPERIMENT_BUCKET_ASSIGNMENT_TIMEOUT_SEC = 1
# Only tested for powers of 10
EXPERIMENT_GRANULARITY = (
    10000  # granularity of bucket sizes, currently we can set to the hundreth decimal place e.g. 2.55 (%)
)
CONTROL_BUCKET = 'control'
HASH_SEED = 42


class ExperimentArguments(ArgumentsBase):
    entity: EntityT[Any]
    buckets: ConstExpr[list[str]]
    bucket_sizes: ConstExpr[list[float]]
    version: ConstExpr[int]
    revision: ConstExpr[int]


class ExperimentsVersion(str, Enum):
    v1 = 'v1'


class ExperimentsProviderArguments(ArgumentsBase):
    id: str
    name: str
    type: str

    def __init__(self, id: str, name: str, type: str):
        self.id = id
        self.name = name
        self.type = type


# NEED FOR MVP
class Experiment(UDFBase[ExperimentArguments, ExperimentT]):
    """
    UDF to create an experiment in Osprey. This UDF resolves an entity to a bucket based
    on the input buckets and the bucket sizes.
    """

    category = UdfCategories.ENGINE

    def __init__(self, validation_context: 'ValidationContext', arguments: ExperimentArguments):
        super().__init__(validation_context, arguments)

        # Make sure we're directly assigned to a feature.
        call_node = arguments.get_call_node()
        parent_node = call_node.parent
        if isinstance(parent_node, grammar.Assign):
            self._feature_name = parent_node.target.identifier
        else:
            add_must_assign_to_variable_error(
                validation_context, message='must assign `Experiment` to a variable', node=call_node
            )

        if arguments.version.value < 0:
            validation_context.add_error(
                message='experiment version must be zero or greater',
                span=arguments.version.argument_span,
            )

        if arguments.revision.value < 0:
            validation_context.add_error(
                message='experiment revision must be zero or greater',
                span=arguments.revision.argument_span,
            )

        if 1 < len(arguments.buckets.value) > 10:
            validation_context.add_error(
                message='number of experiment buckets must be between 1 and 10 inclusive',
                span=arguments.buckets.argument_span,
            )
            return

        if CONTROL_BUCKET not in arguments.buckets.value:
            validation_context.add_error(
                message=f"'{CONTROL_BUCKET}' must be one of the buckets",
                span=arguments.buckets.argument_span,
            )

        if len(arguments.buckets.value) != len(arguments.bucket_sizes.value):
            validation_context.add_error(
                message='buckets and bucket_sizes must have the same number of elements',
                span=arguments.bucket_sizes.argument_span,
            )
            return

        for bucket_size_percentage in arguments.bucket_sizes.value:
            # use decimal because of inaccuracies with floating point arithmetic
            # convert to str first because it will more accurately convert to decimal than a float
            unchecked_bucket_size = Decimal(str(bucket_size_percentage)) * Decimal(EXPERIMENT_GRANULARITY / 100)
            if floor(unchecked_bucket_size) != unchecked_bucket_size:
                validation_context.add_error(
                    message="""
                    experiment bucket size precision is too high,
                    the precision can be at most to the hundredth decimal place
                    """,
                    span=arguments.bucket_sizes.argument_span,
                )

        # convert to whole numbers to deal with calculations more easily
        bucket_sizes = [self.percentage_to_bucket_units(percentage) for percentage in arguments.bucket_sizes.value]
        max_bucket_size = self.get_max_bucket_size(len(arguments.bucket_sizes.value))

        for bucket_size in bucket_sizes:
            if bucket_size > max_bucket_size:
                validation_context.add_error(
                    message=f"""
                    {self.bucket_units_to_percentage(bucket_size)} is over the
                    current max of {self.bucket_units_to_percentage(max_bucket_size)}.
                    Either reduce the bucket size or reduce the number of buckets
                    """,
                    span=arguments.bucket_sizes.argument_span,
                    hint="""
                    max percentage size of each bucket is 100/(# of buckets) rounded
                    down to the nearest hundreth decimal place
                    """,
                )

        self._register_experiment(validation_context=validation_context, experiment_call_node=call_node)

    def _register_experiment(self, validation_context: ValidationContext, experiment_call_node: grammar.Call) -> None:
        cur_experiment_nodes: dict[str, grammar.Call] = validation_context.get_validator_input(ValidateExperiments, {})
        if not cur_experiment_nodes:
            validation_context.set_validator_input(ValidateExperiments, cur_experiment_nodes)
        cur_experiment_nodes.update({self._feature_name: experiment_call_node})

    def execute(self, execution_context: ExecutionContext, arguments: ExperimentArguments) -> ExperimentT:
        bucket = self.get_bucket(self._feature_name, execution_context, arguments)
        return ExperimentT(
            name=self._feature_name,
            entity=arguments.entity,
            buckets=arguments.buckets.value,
            bucket_sizes=arguments.bucket_sizes.value,
            resolved_bucket=bucket,
            version=arguments.version.value,
            revision=arguments.revision.value,
        )

    @staticmethod
    def hash_mod(experiment_name: str, entity_id: str) -> int:
        hash_value: int = mmh3.hash(f'{experiment_name}/{entity_id}', seed=HASH_SEED, signed=False)
        return hash_value % EXPERIMENT_GRANULARITY

    @staticmethod
    def bucket_units_to_percentage(bucket_units: int) -> Decimal:
        return Decimal(bucket_units) / Decimal(EXPERIMENT_GRANULARITY / 100)

    @staticmethod
    def percentage_to_bucket_units(percentage: float) -> int:
        return int(Decimal(str(percentage)) * Decimal(EXPERIMENT_GRANULARITY / 100))

    @staticmethod
    def get_max_bucket_size(num_of_buckets: int) -> int:
        """
        Convert to whole number based on granularity and round down
        """
        return floor(EXPERIMENT_GRANULARITY / num_of_buckets)

    def get_bucket(
        self,
        experiment_name: str,
        execution_context: ExecutionContext,
        arguments: ExperimentArguments,
    ) -> str:
        """
        Gets the bucket the entity should be placed in for experiment `experiment_name`.

        Each bucket reserves EXPERIMENT_GRANULARITY/(# of buckets) "bucket units".
        If our hash falls within the defined size range for a bucket, return the bucket name
        Otherwise, return `NOT_IN_EXPERIMENT_BUCKET`

        If `local_bucketing` is False AND the `entity.type` is of "user" or "guild", then proceed to call experiments
        API to fetch bucket assignment instead of using local methods.
        """

        bucket_unit_index = Experiment.hash_mod(experiment_name, arguments.entity.id)
        max_units_per_bucket = self.get_max_bucket_size(len(arguments.buckets.value))
        bucket_index = bucket_unit_index // max_units_per_bucket
        bucket_unit_offset = max_units_per_bucket * bucket_index
        bucket = (
            NOT_IN_EXPERIMENT_BUCKET
            if (bucket_index + 1 > len(arguments.buckets.value))
            or bucket_unit_index
            >= self.percentage_to_bucket_units(arguments.bucket_sizes.value[bucket_index]) + bucket_unit_offset
            else arguments.buckets.value[bucket_index]
        )
        return bucket

    @classmethod
    def build_cls(cls, key: str) -> Type[Experiment]:
        """Build a subclasss of Experiment keyed off TODO"""
        new_udf_cls = type('ExperimentsBucketAssignment', (cls,), {})
        assert issubclass(new_udf_cls, cls), 'help mypy out a little'

        # return built class
        return new_udf_cls


class ExperimentWhenArguments(ArgumentsBase):
    experiment: ExperimentT
    extra_arguments: dict[str, list[bool]]


class ExperimentWhen(UDFBase[ExperimentWhenArguments, list[bool]]):
    """
    Takes in an experiment and returns the List of bools for the specific bucket
    """

    category = UdfCategories.ENGINE

    def __init__(self, validation_context: 'ValidationContext', arguments: ExperimentWhenArguments) -> None:
        super().__init__(validation_context, arguments)
        self._validate_experimentwhen(validation_context, arguments)

    def _validate_experimentwhen(
        self, validation_context: 'ValidationContext', arguments: ExperimentWhenArguments
    ) -> None:
        """
        Given a call node and experimentwhenarguments verify that:
            - arguments.experiment is a defined experiment
            - the number of buckets defined in experimentwhen is
                the same as the number of buckets in the experiment
        """
        experiment_identifier, experiment_call = self._get_valid_experiment(validation_context, arguments)
        if experiment_call is None:
            return

        experiment_buckets = self._get_buckets_for_experiment(experiment_call)

        expected_buckets = {str(_.value) for _ in experiment_buckets.items}
        provided_buckets = set(arguments.get_extra_arguments_ast())

        # check for missing buckets
        for bucket in expected_buckets - provided_buckets:
            validation_context.add_error(
                message=(f"missing bucket '{bucket}' required from experiment '{experiment_identifier}'"),
                span=arguments.get_call_node().span,
                additional_spans=[experiment_buckets.span],
            )

        # check for unexpected buckets
        for bucket in provided_buckets - expected_buckets:
            validation_context.add_error(
                message=(
                    f"unexpected bucket '{bucket}', this bucket must match"
                    f" a bucket defined in experiment '{experiment_identifier}'"
                ),
                span=arguments.get_argument_ast(bucket).span,
                additional_spans=[experiment_buckets.span],
            )

    def _get_experiments(self, validation_context: 'ValidationContext') -> dict[str, grammar.Call]:
        return validation_context.get_validator_input(ValidateExperiments, dict())

    def _get_valid_experiment(
        self, validation_context: 'ValidationContext', arguments: ExperimentWhenArguments
    ) -> Tuple[str, Optional[grammar.Call]]:
        experiment_arg = arguments.get_argument_ast('experiment')

        assert isinstance(experiment_arg, grammar.Name), 'this should be a Name'
        experiment_identifier = experiment_arg.identifier
        experiment_call = self._get_experiments(validation_context).get(experiment_identifier)

        if experiment_call is None:
            validation_context.add_error(
                message=f"experiment '{experiment_identifier}' not defined",
                span=experiment_arg.span,
            )

        return experiment_identifier, experiment_call

    def _get_buckets_for_experiment(self, experiment: grammar.Call) -> grammar.List:
        experiment_buckets_keyword = experiment.find_argument('buckets')
        assert experiment_buckets_keyword is not None, 'buckets should be defined'
        experiment_buckets = experiment_buckets_keyword.value
        assert isinstance(experiment_buckets, grammar.List), 'buckets should be a List node'
        return experiment_buckets

    def execute(self, execution_context: ExecutionContext, arguments: ExperimentWhenArguments) -> list[bool]:
        resolved_bucket = arguments.experiment.resolved_bucket
        bucket = CONTROL_BUCKET if resolved_bucket is NOT_IN_EXPERIMENT_BUCKET else resolved_bucket
        # validation that the bucket_name is a valid key in extra_arguments should already be done
        return arguments.extra_arguments[bucket]


ExperimentsBucketAssignment = Experiment.build_cls('experiment_bucket_assignment')
