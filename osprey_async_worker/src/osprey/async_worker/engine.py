"""Async Osprey engine — no gevent dependency.

Replaces OspreyEngine's gevent.pool.ThreadPool with stdlib
concurrent.futures.ThreadPoolExecutor for rule compilation.
Provides async execute() that calls the async executor directly.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Type, TypeVar

if TYPE_CHECKING:
    from osprey.worker.lib.data_exporters.validation_result_exporter import BaseValidationResultExporter

from ddtrace.span import Span as TracerSpan
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
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.sources_config import get_config_registry
from osprey.worker.lib.sources_provider_base import BaseSourcesProvider

from osprey.async_worker.executor import execute as async_execute
from osprey.worker.lib.singletons import CONFIG

log = logging.getLogger(__name__)

_DEFAULT_MAX_ASYNC_PER_EXECUTION = 12


class AsyncOspreyEngine:
    """Async rules engine — no gevent dependency.

    Uses concurrent.futures.ThreadPoolExecutor for compilation (CPU-bound)
    and the async executor for action execution.
    """

    def __init__(
        self,
        sources_provider: BaseSourcesProvider,
        udf_registry: UDFRegistry,
        validation_exporter: Optional['BaseValidationResultExporter'] = None,
    ):
        self._sources_provider = sources_provider
        self._udf_registry = udf_registry
        config_registry = get_config_registry()
        self._validator_registry = ValidatorRegistry.instance_with_additional_validators(
            config_registry.get_validator()
        )
        self._thread_pool = ThreadPoolExecutor(max_workers=1)
        self._execution_graph = self._compile_execution_graph_sync()
        self._sources_provider.set_sources_watcher(self._handle_updated_sources)
        self._config_subkey_handler = ConfigSubkeyHandler(config_registry, self._execution_graph.validated_sources)
        self._validation_result_exporter = validation_exporter

    def _compile_execution_graph_sync(self) -> ExecutionGraph:
        """Compile the execution graph synchronously. Used for initial compilation."""
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
            'execution graph compiled: validation %.2fs, compilation %.2fs, total %.2fs',
            validation_time,
            compile_time,
            validation_time + compile_time,
        )
        return execution_graph

    async def compile_execution_graph(self) -> ExecutionGraph:
        """Compile the execution graph in a thread pool (CPU-bound work)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._thread_pool, self._compile_execution_graph_sync)

    def _handle_updated_sources(self) -> None:
        """Called by the sources provider when rules change in etcd."""
        try:
            self._execution_graph = self._compile_execution_graph_sync()
            log.info(f'Compiled new execution graph for sources={self._sources_provider.get_current_sources().hash()}')
        except Exception:
            log.exception(
                f'Failed to compile execution graph for sources={self._sources_provider.get_current_sources().hash()}'
            )
        else:
            self._config_subkey_handler.dispatch_config(self._execution_graph.validated_sources)

        if self._validation_result_exporter is not None:
            try:
                self._validation_result_exporter.send(self._execution_graph.validated_sources)
            except Exception:
                log.exception('Failed to export validation results')

    @property
    def execution_graph(self) -> ExecutionGraph:
        return self._execution_graph

    @property
    def config(self) -> SourcesConfig:
        return self._execution_graph.validated_sources.sources.config

    async def execute(
        self,
        udf_helpers: UDFHelpers,
        action: Action,
        max_concurrent: Optional[int] = None,
        sample_rate: int = 100,
        parent_tracer_span: Optional[TracerSpan] = None,
    ) -> ExecutionResult:
        """Execute an action against the rules using the async executor."""
        if max_concurrent is None:
            max_concurrent = CONFIG.instance().get_int(
                'OSPREY_MAX_ASYNC_PER_EXECUTION', _DEFAULT_MAX_ASYNC_PER_EXECUTION
            )
        return await async_execute(
            self._execution_graph,
            udf_helpers,
            action,
            max_concurrent=max_concurrent,
            sample_rate=sample_rate,
            parent_tracer_span=parent_tracer_span,
        )

    def get_config_subkey(self, model_class: Type[ModelT]) -> ModelT:
        return self._config_subkey_handler.get_config_subkey(model_class)

    def watch_config_subkey(self, model_class: Type[ModelT], update_callback: Callable[[ModelT], None]) -> None:
        self._config_subkey_handler.watch_config_subkey(model_class, update_callback)

    def get_known_feature_locations(self) -> List:
        identifier_index: IdentifierIndex = self._execution_graph.validated_sources.get_validator_result(
            UniqueStoredNames
        )
        return [
            {
                'name': name,
                'source_path': span.source.path,
                'source_line': span.start_line,
            }
            for name, span in identifier_index.items()
        ]

    def get_known_action_names(self) -> Set[str]:
        return {
            Path(source.path).stem for source in self.execution_graph.validated_sources.sources.glob('actions/*.sml')
        }

    def get_rule_to_info_mapping(self) -> Dict[str, str]:
        return self._execution_graph.validated_sources.get_validator_result(RuleNameToDescriptionMapping)

    def shutdown(self) -> None:
        """Shutdown the compilation thread pool."""
        self._thread_pool.shutdown(wait=True)
