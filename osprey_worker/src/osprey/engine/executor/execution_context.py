import json
import logging
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Type,
    TypeAlias,
    TypeVar,
)

from google.protobuf.timestamp_pb2 import Timestamp
from osprey.engine.ast.grammar import ASTNode, Load, Name, Source
from osprey.engine.ast.printer import print_ast
from osprey.engine.executor.custom_extracted_features import (
    CustomExtractedFeature,
)
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.external_service_utils import ExternalService, ExternalServiceAccessor, KeyT, ValueT
from osprey.engine.executor.topological_sorter import TopologicalSorter
from osprey.engine.executor.udf_execution_helpers import HasHelperInternal, HelperT, UDFHelpers
from osprey.engine.language_types.effects import (
    EffectBase,
    EffectToCustomExtractedFeatureBase,
)
from osprey.engine.language_types.post_execution_convertible import PostExecutionConvertible
from osprey.engine.language_types.verdicts import VerdictEffect
from osprey.engine.utils.types import add_slots, cached_property
from osprey.rpc.common.v1.verdicts_pb2 import Verdicts
from osprey.worker.lib.osprey_shared.labels import EntityLabelMutation
from result import Result, UnwrapError

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

logger = logging.getLogger(__name__)

NodeResult: TypeAlias = Result[object, None]


class NodeFailurePropagationException(Exception):
    """Indicates that a node depended on failed, and therefore the current node should propagate that failure too."""


class ExpectedUdfException(Exception):
    """Exception for a UDF that failed, so we don't want to execute any nodes that depend on it, but is expected so
    we don't want to report it."""


class GeventTimeoutException(Exception):
    """Indicates Gevent timed out this node."""


class ExternalServiceException(Exception):
    """Indicates that an external service call failed or returned unexpected data."""



class ExecutionContext:
    """The execution context stores any outputs or intermediate state of an execution."""

    __slots__ = (
        '_action',
        '_data',
        '_input_encoding',
        '_execution_graph',
        '_outputs',
        '_pending_executions',
        '_resolved_node_values',
        '_visited_executions',
        '_effects',
        '_udf_helpers',
        '_external_service_accessors_by_getter_id',
        '_dependency_dag',
        '_chain_by_id',
        '_custom_extracted_features',
    )

    def __init__(self, execution_graph: ExecutionGraph, action: 'Action', helpers: UDFHelpers):
        self._action = action
        self._data = action.data
        self._input_encoding = action.encoding
        self._execution_graph = execution_graph
        self._udf_helpers: UDFHelpers = helpers
        self._outputs: Dict[str, Any] = {}
        self._pending_executions: Set[DependencyChain] = set()
        self._resolved_node_values: Dict[int, NodeResult] = {}
        self._visited_executions: Set[DependencyChain] = set()
        # a k/v store of effects, by effect type
        self._effects: DefaultDict[Type[EffectBase], List[EffectBase]] = defaultdict(list)
        self._external_service_accessors_by_getter_id: Dict[int, ExternalServiceAccessor[Any, Any]] = {}
        self._dependency_dag = TopologicalSorter()
        self._chain_by_id: Dict[int, DependencyChain] = {}
        # feature name -> serializable feature
        self._custom_extracted_features: Dict[str, Any] = {}

        self.enqueue_source(execution_graph.get_entry_point())

    @property
    def validated_sources(self) -> 'ValidatedSources':
        return self._execution_graph.validated_sources

    def resolved(self, node: ASTNode, return_none_for_failed_values: bool = False) -> object:
        """Returns the resolved value of a given node.

        If the node has yet to be resolved, this throws an exception. If the node (or one of its dependencies)
        failed, will either raise a NodeFailurePropagationException (default) or return None (if
        return_none_for_failed_values is True).
        """
        # We need to check this on the original node, not (say) the assignment node if this is a Name.
        should_unwrap = self._execution_graph.should_unwrap(node)

        if isinstance(node, Name):
            node = self.get_name_node(node)

        try:
            value = self._resolved_node_values[id(node)].unwrap()
            if should_unwrap:
                assert isinstance(value, PostExecutionConvertible), (value, type(value))
                value = value.to_post_execution_value()
            return value
        except UnwrapError:
            if return_none_for_failed_values:
                return None
            else:
                raise NodeFailurePropagationException()

    def get_name_node(self, name: Name) -> ASTNode:
        """Returns the node that is responsible for resolving a given Loaded name."""
        assert isinstance(name.context, Load)
        return self._execution_graph.get_assignment_dependency_chain(name).executor.node

    def set_resolved_value(self, chain: DependencyChain, value: NodeResult) -> None:
        """Called by the main executor once a node has been resolved, to store its value for dependent executors."""
        self._resolved_node_values[id(chain.executor.node)] = value
        self._dependency_dag.done(id(chain))

    def set_output_value(self, key: str, value: Any) -> None:
        """Called by the assignment node executor to store an output key/value pair."""
        self._outputs[key] = value

    def get_outputs(self) -> Dict[str, Any]:
        """Gets the output of the executor. This should only be called by the main executor, once execution has
        finished. Under no circumstance should anything else call this function."""
        return {
            k: v.to_post_execution_value() if isinstance(v, PostExecutionConvertible) else v
            for k, v in self._outputs.items()
        }

    def get_data(self) -> Dict[str, Any]:
        """Gets the input-data that the execution context is currently being invoked upon."""
        return self._data

    def get_data_encoding(self) -> str:
        """Gets the encoding type of the Osprey action data (e.g. json or proto)"""
        return self._input_encoding

    def get_secret_data(self) -> Dict[str, Any]:
        """Gets the secret input-data that the execution context is currently being invoked upon."""
        return self._action.secret_data

    def get_action_name(self) -> str:
        """Returns the action name that the execution context is currently being invoked upon."""
        return self._action.action_name

    def get_action_time(self) -> datetime:
        """Returns the time of the action that the execution context is currently being invoked upon."""
        return self._action.timestamp

    def enqueue_source(self, source: Source) -> None:
        sorted_dependency_chain = self._execution_graph.get_sorted_dependency_chain(source)

        for chain in sorted_dependency_chain:
            chainid = id(chain)
            if self._dependency_dag.already_added(chainid):
                continue
            self._dependency_dag.add(chainid, *(id(pred) for pred in chain.dependent_on))
            self._chain_by_id[chainid] = chain

        self._dependency_dag.prepare()

    def get_ready_to_execute(self) -> Sequence[DependencyChain]:
        ready_nodeids = self._dependency_dag.get_ready()
        ready = []
        for nodeid in ready_nodeids:
            ready.append(self._chain_by_id[nodeid])

        return ready

    def add_effect(self, effect: EffectBase) -> None:
        self._effects[type(effect)].append(effect)

    def get_effects(self) -> Mapping[Type[EffectBase], Sequence[EffectBase]]:
        return dict(self._effects)

    def add_custom_extracted_feature(
        self, custom_extracted_feature: CustomExtractedFeature[Any], error_on_duplicate_key: bool = True
    ) -> None:
        """
        Adds a custom extracted feature to the execution context.

        If the feature already exists, this method will error unless `error_on_duplicate_key` is False.
        """
        if self._custom_extracted_features.get(f'__{custom_extracted_feature.feature_name()}') is not None:
            if error_on_duplicate_key:
                raise ValueError(
                    f'custom extracted feature `{custom_extracted_feature.feature_name()}` already exists on the execution context and is being overridden'
                )
        feature = custom_extracted_feature.get_serializable_feature()
        if feature is not None:
            self._custom_extracted_features[f'__{custom_extracted_feature.feature_name()}'] = feature
        else:
            self._custom_extracted_features.pop(f'__{custom_extracted_feature.feature_name()}', None)

    def add_custom_extracted_features(
        self, custom_extracted_features: Iterable[CustomExtractedFeature[Any]], error_on_duplicate_keys: bool = True
    ) -> None:
        for custom_extracted_feature in custom_extracted_features:
            self.add_custom_extracted_feature(custom_extracted_feature, error_on_duplicate_keys)

    def get_extracted_features(self) -> Dict[str, Any]:
        """
        Returns a `Dict[str, Any]` of all of the extracted features, including the base extracted features ("outputs")
        and any custom extracted features supplied via `add_custom_extracted_feature` or `add_custom_extracted_features`.

        If any custom extracted features have the same key as a base extracted feature, the custom extracted feature will
        be excluded and an error will be logged.
        """

        # first we collect all of the custom extracted features from the effects and adds them to the context.
        # this is idempotent, because we override `error_on_duplicate_key` to False when adding the custom extracted features
        for effect_type, effect_list in self._effects.items():
            # If the EffectToCustomExtractedFeatureBase matches, then we can collect these effect values
            # into a custom extracted feature meant for lists and pack them into the extracted_features dict~
            if issubclass(effect_type, EffectToCustomExtractedFeatureBase):
                # extra type asserting
                type_checked_list = [effect for effect in effect_list if isinstance(effect, effect_type)]
                if type_checked_list != effect_list:
                    # we don't want to raise an error because that would break the creation of the execution results.
                    # instead we will log an error and just ignore this effect.
                    logger.error(
                        f'{effect_type.__name__} effect list contained an invalid type! this should not be possible'
                    )
                    continue
                custom_extracted_feature = effect_type.build_custom_extracted_feature_from_list(type_checked_list)
                self.add_custom_extracted_feature(custom_extracted_feature, error_on_duplicate_key=False)
                # NOTE(ayubun): to add more types of effect-supported custom extracted features, we can add more `elif`s here~

        # NOTE(ayubun): we collect outputs as the base extracted features, and compare against all custom extracted features
        # for any conflicting keys. we append a double underscore to the custom extracted feature keys to do our best to avoid conflicts.
        # i'm *pretty sure* that it's not possible for double underscored features to be outputted by the executor, but might as well be safe!

        base_extracted_features = self.get_outputs()
        conflicting_keys = set(base_extracted_features.keys()).intersection(set(self._custom_extracted_features.keys()))
        if conflicting_keys:
            logger.error(
                'INVARIANT: custom extracted features cannot have the same keys as base extracted features! D:'
                'the following custom extracted features will be exluded: '
                f'{", ".join(conflicting_keys)}'
            )

        # NOTE(ayubun): finally, we can return the base and custom combined. with python's union operator, dicts on the the right side
        # will override ones on the left side in the event of any key conflicts (*・ｖ・)
        return self._custom_extracted_features | base_extracted_features

    def get_udf_helper(self, udf: HasHelperInternal[HelperT]) -> HelperT:
        """Returns the helper object for a UDF that needs outside functionality to do its job."""
        return self._udf_helpers.get_udf_helper(udf)

    def get_external_service_accessor(
        self, external_service: ExternalService[KeyT, ValueT]
    ) -> ExternalServiceAccessor[KeyT, ValueT]:
        """Given an external service, wraps that service in an accessor that ensures that requests to the service are
        cached and debounced by key within this execution."""
        # No need to lock since not doing any IO
        accessor = self._external_service_accessors_by_getter_id.get(id(external_service))
        if accessor is None:
            accessor = ExternalServiceAccessor(external_service)
            self._external_service_accessors_by_getter_id[id(external_service)] = accessor

        return accessor


_ActionT = TypeVar('_ActionT', bound='Action')


@add_slots
@dataclass
class Action:
    """An action is data that will be classified by the engine. Action has arbitrary data, and a unique ID that will
    identify it to the rest of the system."""

    action_id: int
    action_name: str
    data: Dict[str, Any]
    timestamp: datetime
    secret_data: Dict[str, Any] = field(default_factory=dict)
    encoding: str = 'unknown'

    @classmethod
    def from_dict(cls: Type[_ActionT], d: Dict[str, Any]) -> '_ActionT':
        return cls(
            action_id=int(d['action_id']),
            action_name=d['action_name'],
            data=d['data'],
            secret_data=d.get('secret', {}),
            timestamp=datetime.fromisoformat(d['timestamp']),
        )

    def to_dict(self) -> Dict[str, Any]:
        # secret data intentionally left out
        return {
            'action_id': self.action_id,
            'action_name': self.action_name,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
        }

    @cached_property
    def data_json(self) -> str:
        """Convenience method for returning the action data json to avoid serializing multiple times,
        if this result is consumed by multiple output streams."""
        return json.dumps(self.data)


@add_slots
@dataclass
class NodeErrorInfo:
    """Contains information about a node's failed execution."""

    error: BaseException
    node: ASTNode


@add_slots
@dataclass
class ExecutionResult:
    """The result of the execution of an action."""

    extracted_features: Dict[str, Any]
    action: Action
    # A k/v store of effects, by effect class type
    effects: Mapping[Type[EffectBase], Sequence[EffectBase]]
    error_infos: Sequence[NodeErrorInfo]
    # leaving it up to the users of `validator_results`, but maybe we want to enforce
    # some kind of typing when outputing `validator_results`
    validator_results: Dict[Any, Any] = field(default_factory=dict)
    sample_rate: int = 100

    def add_custom_extracted_feature(self, custom_extracted_feature: CustomExtractedFeature[Any]) -> None:
        name = custom_extracted_feature.feature_name()
        packed = custom_extracted_feature.get_serializable_feature()

        if packed is None:
            return

        if self.extracted_features.get(name) is not None:
            raise ValueError(f'BUG: custom extracted feature `{name}` already exists on the execution result')

        self.extracted_features[name] = packed

    def add_custom_extracted_features(self, custom_extracted_features: Sequence[CustomExtractedFeature[Any]]) -> None:
        for custom_extracted_feature in custom_extracted_features:
            self.add_custom_extracted_feature(custom_extracted_feature)

    @cached_property
    def verdicts(self) -> Sequence[VerdictEffect]:
        return [
            osprey_verdict_effect
            for osprey_verdict_effect in self.effects.get(VerdictEffect, [])
            if isinstance(osprey_verdict_effect, VerdictEffect)
        ].copy()

    def _get_timestamp_pb2_proto(self) -> Timestamp:
        timestamp = Timestamp()
        timestamp.FromDatetime(dt=self.action.timestamp)
        return timestamp

    def get_verdicts_pb2_proto(self) -> Verdicts:
        """
        returns a pb2 protobuf of the verdicts declared by the action, along with some extra metadata~
        ╰(*°▽°*)╯
        """
        return Verdicts(
            action_id=self.action.action_id,
            action_name=self.action.action_name,
            verdicts=[v.verdict for v in self.verdicts],
            timestamp=self._get_timestamp_pb2_proto(),
        )

    @cached_property
    def extracted_features_json(self) -> str:
        """Convenience method for returning the extracted features json to avoid serializing multiple times,
        if this result is consumed by multiple output streams."""
        return json.dumps(self.extracted_features)

    @cached_property
    def error_traces_json(self) -> str:
        formatted_errors = []
        for error_info in self.error_infos:
            traceback_lines = traceback.format_exception(
                type(error_info.error), error_info.error, error_info.error.__traceback__
            )
            span = error_info.node.span
            if span is None:
                span_text = '<unknown location>'
            else:
                span_text = f'{span.source.path}:{span.start_line}:{span.start_pos}'
            rules_source_location = f'{span_text} - {print_ast(error_info.node)}'

            formatted_errors.append(
                {'traceback': ''.join(traceback_lines), 'rules_source_location': rules_source_location}
            )
        return json.dumps(formatted_errors)
