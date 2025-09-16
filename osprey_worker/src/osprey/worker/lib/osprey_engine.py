import logging
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Callable, Dict, Generic, List, Optional, Set, Type, TypeVar

import gevent
import gevent.pool
from ddtrace.span import Span as TracerSpan
from gevent.threadpool import ThreadPool
from osprey.engine.ast.grammar import Assign, Span
from osprey.engine.ast.sources import Sources, SourcesConfig
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.ast_validator.validators.feature_name_to_entity_type_mapping import (
    FeatureNameToEntityTypeMapping,
)
from osprey.engine.ast_validator.validators.rule_name_to_description_mapping import (
    RuleNameToDescriptionMapping,
)
from osprey.engine.ast_validator.validators.unique_stored_names import (
    IdentifierIndex,
    UniqueStoredNames,
)
from osprey.engine.ast_validator.validators.validate_static_types import (
    ValidateStaticTypes,
)
from osprey.engine.config.config_subkey_handler import ConfigSubkeyHandler, ModelT
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.executor import execute
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.udf.registry import UDFRegistry
from osprey.engine.utils.periodic_execution_yielder import periodic_execution_yield
from osprey.engine.utils.types import add_slots
from osprey.worker.lib.data_exporters.validation_result_exporter import (
    BaseValidationResultExporter,
    NullValidationResultExporter,
)
from osprey.worker.lib.sources_config import get_config_registry
from osprey.worker.lib.utils.input_stream_ready_signaler import InputStreamReadySignaler
from pydantic.main import BaseModel

from .singletons import CONFIG
from .sources_provider import BaseSourcesProvider, EtcdSourcesProvider, StaticSourcesProvider

log = logging.getLogger(__name__)


_DEFAULT_MAX_ASYNC_PER_EXECUTION = 12


_ModelT = TypeVar('_ModelT', bound=BaseModel)


@add_slots
@dataclass
class _ConfigSubkeyHandler(Generic[_ModelT]):
    model_class: Type[_ModelT]
    callbacks: List[Callable[[_ModelT], None]]


@dataclass
class FeatureLocation:
    name: str
    source_path: str
    source_line: int
    source_snippet: str


class OspreyEngine:
    """The entry-point to the osprey rules engine, providing an interface to execute the rules specified by a
    `BaseSourcesProvider`."""

    def __init__(
        self,
        sources_provider: BaseSourcesProvider,
        udf_registry: UDFRegistry,
        should_yield_during_compilation: bool = False,
        validation_exporter: BaseValidationResultExporter = NullValidationResultExporter(),
    ):
        self._sources_provider = sources_provider
        self._should_yield_during_compilation = should_yield_during_compilation
        self._udf_registry = udf_registry
        config_registry = get_config_registry()
        # Note: _validator_registry must be set before calling _compile_execution_graph
        self._validator_registry = ValidatorRegistry.instance_with_additional_validators(
            config_registry.get_validator()
        )
        self._execution_graph_compilation_thread_pool = ThreadPool(maxsize=1)
        self._execution_graph = self._compile_execution_graph(disable_periodic_yield=True)
        self._sources_provider.set_sources_watcher(self._handle_updated_sources)
        self._config_subkey_handler = ConfigSubkeyHandler(config_registry, self._execution_graph.validated_sources)
        self._validation_result_exporter = validation_exporter

    def _compile_execution_graph(self, disable_periodic_yield: bool = False) -> ExecutionGraph:
        def _do_compile_execution_graph() -> ExecutionGraph:
            with periodic_execution_yield(
                on=self._should_yield_during_compilation and not disable_periodic_yield,
                execution_time_ms=5,
                yield_time_ms=25,
            ):
                sources = self._sources_provider.get_current_sources()

                start_time = time()
                validated_sources = validate_sources(
                    sources, udf_registry=self._udf_registry, validator_registry=self._validator_registry
                )
                validation_time = time() - start_time

                start_time = time()
                execution_graph = compile_execution_graph(validated_sources)
                compile_time = time() - start_time

            log.debug(
                'execution graph has been re-compiled: validation took %.2f sec, '
                'compilation took %.2f sec, total: %.2f',
                validation_time,
                compile_time,
                validation_time + compile_time,
            )

            return execution_graph

        return self._execution_graph_compilation_thread_pool.apply(_do_compile_execution_graph)

    def _handle_updated_sources(self) -> None:
        # noinspection PyBroadException
        try:
            self._execution_graph = self._compile_execution_graph()
            log.info(f'Compiled new execution graph for sources={self._sources_provider.get_current_sources().hash()}')
        except Exception:
            log.exception(
                f'Failed to compile execution graph for sources={self._sources_provider.get_current_sources().hash()}'
            )
        else:
            # Only do this if no exception occurred above
            self._config_subkey_handler.dispatch_config(self._execution_graph.validated_sources)

        # noinspection PyBroadException
        # try to send validation results, should not block osprey_engine if this fails
        try:
            self._validation_result_exporter.send(self._execution_graph.validated_sources)
        except Exception:
            log.exception(
                f"""
                Failed to export validation results for source_hash =
                {self._sources_provider.get_current_sources().hash()}"""
            )

    @property
    def execution_graph(self) -> ExecutionGraph:
        return self._execution_graph

    @property
    def config(self) -> SourcesConfig:
        return self._execution_graph.validated_sources.sources.config

    def get_known_feature_locations(self) -> List[FeatureLocation]:
        """Gets the known feature locations from the rules engine."""

        def _should_extract(span: Span) -> bool:
            """Check if the given span is part of an Assign node that should be extracted"""
            maybe_assign = span.parent_ast_node()
            return bool(maybe_assign.should_extract if isinstance(maybe_assign, Assign) else True)

        identifier_index: IdentifierIndex = self._execution_graph.validated_sources.get_validator_result(
            UniqueStoredNames
        )

        return [
            FeatureLocation(
                name=name,
                source_path=span.source.path,
                source_line=span.start_line,
                source_snippet=extract_source_snippet(span),
            )
            for name, span in identifier_index.items()
            if _should_extract(span)
        ]

    def get_known_action_names(self) -> Set[str]:
        """Gets known action names by looking at the sources for files in the `actions/` directory. It is assumed,
        that .sml files in that directory map to a given action name.

        This is a convention, and not a constraint however. It's assumed that many rule-sets will have
        something similar to the following in their `main.sml` file.

        ```py
        ActionName = GetActionName()
        Require(rule=f'actions/{ActionName}.sml')
        ```
        """
        return {
            Path(source.path).stem for source in self.execution_graph.validated_sources.sources.glob('actions/*.sml')
        }

    def get_rule_to_info_mapping(self) -> Dict[str, str]:
        """Returns a mapping from 'rule name' -> 'rule description' for each feature that is a rule declaration."""
        return self._execution_graph.validated_sources.get_validator_result(RuleNameToDescriptionMapping)

    def execute(
        self,
        udf_helpers: UDFHelpers,
        action: Action,
        sample_rate: int = 100,
        parent_tracer_span: Optional[TracerSpan] = None,
    ) -> ExecutionResult:
        """Given input action, execute it against the execution engine and return the result."""
        return execute(
            self._execution_graph,
            udf_helpers,
            action,
            gevent.pool.Pool(_DEFAULT_MAX_ASYNC_PER_EXECUTION),
            sample_rate,
            parent_tracer_span,
        )

    def watch_config_subkey(self, model_class: Type[ModelT], update_callback: Callable[[ModelT], None]) -> None:
        """Register to watch for updates to the given subkey.

        See ConfigSubkeyHandler.watch_config_subkey for more details.
        """
        self._config_subkey_handler.watch_config_subkey(model_class, update_callback)

    def get_config_subkey(self, model_class: Type[ModelT]) -> ModelT:
        """Returns the parsed model object for the subkey.

        See ConfigSubkeyHandler.watch_config_subkey for more details.
        """
        return self._config_subkey_handler.get_config_subkey(model_class)

    def get_feature_name_to_entity_type_mapping(self) -> Dict[str, str]:
        """Returns a mapping from 'feature name' -> 'entity type' for each feature that holds an entity."""
        return self._execution_graph.validated_sources.get_validator_result(FeatureNameToEntityTypeMapping)

    def get_post_execution_feature_name_to_value_type_mapping(self) -> Dict[str, type]:
        """Returns a mapping from 'feature name' -> 'value type' for each feature."""
        post_execution_name_to_type_and_span = ValidateStaticTypes.to_post_execution_types(
            self._execution_graph.validated_sources.get_validator_result(ValidateStaticTypes)
        )

        return {
            name: type_and_span.type
            for name, type_and_span in post_execution_name_to_type_and_span.items()
            if type_and_span.should_extract
        }


def get_sources_provider(
    rules_path: Optional[Path] = None,
    input_stream_ready_signaler: Optional[InputStreamReadySignaler] = None,
) -> BaseSourcesProvider:
    """Creates the osprey engine sources provider. If a path is provided, will use a `StaticSourcesProvider` which will
    load sources from the given location on disk. Otherwise, will use the `EtcdSourcesProvider` to dynamically
    load sources and keep them up to date."""

    if rules_path is None:
        config = CONFIG.instance()
        return EtcdSourcesProvider(
            etcd_key=config.get_str('OSPREY_ETCD_SOURCES_PROVIDER_KEY', '/config/osprey/rules-sink-sources'),
            input_stream_ready_signaler=input_stream_ready_signaler,
        )
    else:
        return StaticSourcesProvider(sources=Sources.from_path(Path(rules_path)))


def should_yield_during_compilation() -> bool:
    """Periodically sleep when validating and compiling osprey rules source files"""
    config = CONFIG.instance()
    return config.get_bool('OSPREY_PERIODIC_YIELD_DURING_COMPILATION', False)


def extract_source_snippet(span: Span) -> str:
    """Extracts a snippet, handling multi-line statements with parentheses."""
    lines = span.source.contents.splitlines()
    if not (0 <= span.start_line < len(lines)):
        return ''

    extracted_snippet = []
    open_parens = 0
    line_index = span.start_line - 1
    start_col = span.start_pos

    while line_index < len(lines):
        line = lines[line_index]

        snippet_part = line[start_col:] if line_index == span.start_line else line

        extracted_snippet.append(snippet_part)

        open_parens += snippet_part.count('(')
        open_parens -= snippet_part.count(')')

        if open_parens <= 0:
            break

        line_index += 1
        start_col = span.start_pos

    return '\n'.join(extracted_snippet)
