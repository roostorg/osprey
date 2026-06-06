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
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Type, TypedDict, TypeVar

if TYPE_CHECKING:
    from osprey.worker.lib.data_exporters.validation_result_exporter import BaseValidationResultExporter

from ddtrace.span import Span as TracerSpan
from osprey.engine.ast.ast_utils import iter_nodes
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
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.sources_config import get_config_registry
from osprey.worker.lib.sources_provider_base import BaseSourcesProvider

from osprey.async_worker.executor import execute as async_execute
from osprey.worker.lib.singletons import CONFIG

log = logging.getLogger(__name__)

_DEFAULT_MAX_ASYNC_PER_EXECUTION = 12


class FeatureLocation(TypedDict):
    """Where a stored-name identifier is declared in the rule sources.

    Returned by :meth:`AsyncOspreyEngine.get_known_feature_locations` so
    out-of-process consumers can render the rules-engine feature catalog
    without re-deriving these positions.
    """

    name: str
    source_path: str
    source_line: int
    source_snippet: str


def _extract_source_snippet(span: Span) -> str:
    """Three-line snippet around `span`. Mirrors osprey_engine.extract_source_snippet."""
    src = span.source.contents
    lines = src.splitlines()
    start = max(span.start_line - 2, 0)
    end = min(span.start_line + 1, len(lines))
    return '\n'.join(lines[start:end])


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
        desired_hash = self._sources_provider.get_current_sources().hash()
        try:
            new_graph = await self.compile_execution_graph()
        except Exception:
            log.exception(f'Failed to compile execution graph for sources={desired_hash}')
            metrics.increment(
                'osprey.rules_compile_failed',
                tags=[f'desired:{desired_hash[:16]}'],
            )
            return

        # Atomic swap. In-flight actions captured the old graph by reference at
        # rules_sink.classify_one start and continue to use it until they finish;
        # only newly-arriving actions read the new graph. Safe regardless of
        # whether the input stream is paused.
        #
        # We deliberately do NOT call gc.collect() here. Forcing a full gen-2
        # collection promotes every surviving object in the process into gen 2,
        # which makes subsequent automatic collections during action processing
        # measurably more expensive.
        #
        # The old graph contains refcycles between AST roots and their children
        # via ASTNode.parent back-pointers (see osprey/engine/ast/grammar.py),
        # so plain refcount alone cannot reclaim it. Without intervention the
        # old graph persists until gen-2 GC catches the cycle, and across many
        # rule recompiles the resulting GC pressure raises per-message CPU.
        #
        # Fix: after the swap, walk the OLD graph's AST and null `parent`
        # pointers — but ONLY on sources whose ast_root is not shared with the
        # NEW graph. `parsed_ast_root_cache` in osprey/engine/ast/grammar.py
        # memoizes ast_root by Source content, so unchanged source files share
        # the same ast_root between graphs. Nulling parents on shared nodes
        # would corrupt the new graph's AST.
        old_graph = self._execution_graph
        self._execution_graph = new_graph

        # Confirm to the provider which sources are now actually live so it dedups
        # future no-op re-deliveries against what we APPLIED (not just received).
        # The compile-failure path above returns early without marking, so a
        # transient failure self-heals on the next etcd re-delivery.
        self._sources_provider.mark_sources_applied(new_graph.validated_sources.sources.hash())

        log.info(f'Compiled new execution graph for sources={self._sources_provider.get_current_sources().hash()}')
        self._config_subkey_handler.dispatch_config(self._execution_graph.validated_sources)

        if self._validation_result_exporter is not None:
            try:
                self._validation_result_exporter.send(self._execution_graph.validated_sources)
            except Exception:
                log.exception('Failed to export validation results')

        self._break_old_graph_cycles(old_graph, new_graph)

    @staticmethod
    def _break_old_graph_cycles(old_graph: ExecutionGraph, new_graph: ExecutionGraph) -> None:
        """Null `parent` back-pointers on every AST node in the discarded graph
        so plain refcount can reclaim it without waiting for gen-2 GC.

        Skips any source whose ast_root is shared with the new graph — those
        come from the module-level ``parsed_ast_root_cache`` and mutating them
        would corrupt the new graph the engine just swapped in.

        Best-effort: any exception here is logged and swallowed. A leaked old
        graph is wasteful but not incorrect.
        """
        try:
            new_root_ids = {id(s.ast_root) for s in new_graph.validated_sources.sources}
            count = 0
            for source in old_graph.validated_sources.sources:
                if id(source.ast_root) in new_root_ids:
                    continue
                for node in iter_nodes(source.ast_root):
                    node.parent = None
                    count += 1
            log.debug('broke parent pointers on %d AST nodes from old graph', count)
        except Exception:
            log.exception('failed to break cycles on old execution graph')

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

    def get_known_feature_locations(self) -> List[FeatureLocation]:
        """Return locations of named identifiers that the rules engine extracts.

        Mirrors osprey.worker.lib.osprey_engine.OspreyEngine.get_known_feature_locations:
        filters the UniqueStoredNames result by the parent Assign node's
        should_extract flag so only identifiers the engine actually extracts make
        it into the result.

        Returns FeatureLocation TypedDicts (JSON-shaped) — the gevent engine
        used a dataclass; this surface stays a plain dict so consumers can
        serialize without conversion while still getting precise types.
        """

        def _should_extract(span: Span) -> bool:
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
                source_snippet=_extract_source_snippet(span),
            )
            for name, span in identifier_index.items()
            if _should_extract(span)
        ]

    def get_known_action_names(self) -> Set[str]:
        return {
            Path(source.path).stem for source in self.execution_graph.validated_sources.sources.glob('actions/*.sml')
        }

    def get_rule_to_info_mapping(self) -> Dict[str, str]:
        return self._execution_graph.validated_sources.get_validator_result(RuleNameToDescriptionMapping)

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

    def shutdown(self) -> None:
        """Shutdown the compilation thread pool."""
        self._thread_pool.shutdown(wait=True)
