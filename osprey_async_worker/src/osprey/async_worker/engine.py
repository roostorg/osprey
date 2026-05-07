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
from osprey.engine.utils.periodic_execution_yielder import periodic_execution_yield
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
        should_yield_during_compilation: bool = False,
        validation_exporter: Optional['BaseValidationResultExporter'] = None,
    ):
        self._sources_provider = sources_provider
        self._udf_registry = udf_registry
        self._should_yield_during_compilation = should_yield_during_compilation
        config_registry = get_config_registry()
        self._validator_registry = ValidatorRegistry.instance_with_additional_validators(
            config_registry.get_validator()
        )
        self._thread_pool = ThreadPoolExecutor(max_workers=1)
        # Initial compile runs without periodic yields — there is no in-flight
        # work yet to protect, and we want fast cold-start.
        self._execution_graph = self._compile_execution_graph_sync(yield_during_compile=False)
        self._sources_provider.set_sources_watcher(self._handle_updated_sources)
        self._config_subkey_handler = ConfigSubkeyHandler(config_registry, self._execution_graph.validated_sources)
        self._validation_result_exporter = validation_exporter

    def _compile_execution_graph_sync(self, yield_during_compile: bool = True) -> ExecutionGraph:
        """Compile the execution graph synchronously.

        When ``yield_during_compile`` is True (the default for recompiles),
        wraps the work in ``periodic_execution_yield`` which causes the
        compile thread to ``time.sleep`` periodically, releasing the GIL so
        the asyncio event loop can keep servicing in-flight tasks. This
        mirrors what the gevent engine does via the same context manager.

        Without this, the compile thread holds the GIL contiguously for ~7s,
        and the asyncio main thread (running the event loop) gets starved
        of CPU even though it's only ~5ms of bytecode releases away from
        running. With CFS throttling on the asyncio worker pods, that
        starvation compounds — compile pegs CPU, CFS throttles, and every
        in-flight coroutine stalls until compile finishes.

        With yields: compile takes ~6× longer wall-clock (~42s) but uses
        only ~16% of one core's CPU duty cycle, leaving plenty of headroom
        for the rest of the worker.
        """
        with periodic_execution_yield(
            on=yield_during_compile and self._should_yield_during_compilation,
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

    async def _handle_updated_sources(self) -> None:
        """Called by the sources provider when rules change in etcd.

        Runs the compile in self._thread_pool rather than on the event loop
        so the loop stays free during compile and in-flight gRPC tasks can
        drain and release their pinned response buffers.
        """
        try:
            new_graph = await self.compile_execution_graph()
        except Exception:
            log.exception(
                f'Failed to compile execution graph for sources={self._sources_provider.get_current_sources().hash()}'
            )
            return

        # Atomic swap. In-flight actions captured the old graph by reference at
        # rules_sink.classify_one start and continue to use it until they finish;
        # only newly-arriving actions read the new graph. Safe regardless of
        # whether the input stream is paused. Reference counting reclaims the
        # old graph + compile intermediates as their last reference drops.
        #
        # We deliberately do NOT call gc.collect() here. Forcing a full gen-2
        # collection promotes every surviving object in the process into gen 2,
        # which makes subsequent automatic collections during action processing
        # measurably more expensive. With the previous gc.collect ×2 in place
        # (added in cmttt/osprey#27) we observed sustained avg per-pod CPU
        # 2.4× elevated for tens of minutes after each rule deploy. A pod
        # rolling restart immediately recovered baseline CPU. The gevent
        # engine does the swap without an explicit gc.collect and recovers
        # cleanly; mirror that.
        self._execution_graph = new_graph

        log.info(f'Compiled new execution graph for sources={self._sources_provider.get_current_sources().hash()}')
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
