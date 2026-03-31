"""Minimal async worker CLI for Phase 0 validation.

No monkey patching. No gevent. Uses asyncio event loop.
Supports static rules files for testing without etcd.
"""

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import click
from osprey.engine.executor.execution_context import Action
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.config import Config
from osprey.worker.lib.instruments import set_worker_type_tag
from osprey.worker.lib.osprey_engine import OspreyEngine, get_sources_provider, should_yield_during_compilation
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.sinks.utils.acking_contexts import NoopAckingContext

from osprey.async_worker.sinks.sink.input_stream import AsyncBaseInputStream, AsyncStaticInputStream
from osprey.async_worker.sinks.sink.output_sink import AsyncStdoutOutputSink
from osprey.async_worker.sinks.sink.rules_sink import AsyncRulesSink

logger = get_logger(__name__)


def init_config() -> Config:
    config = CONFIG.instance()
    config.configure_from_env()
    set_worker_type_tag('async')
    return config


def bootstrap_stdlib_engine(rules_path: str) -> Tuple[OspreyEngine, UDFHelpers]:
    """Bootstrap engine with only stdlib UDFs — no external plugins, no Postgres, no labels.

    This avoids loading example_plugins or discord plugins that require database connections.
    """
    from osprey.worker._stdlibplugin.udf_register import register_udfs as stdlib_register_udfs
    from osprey.worker._stdlibplugin.validator_regsiter import register_ast_validators as stdlib_register_validators
    from osprey.engine.ast_validator import ValidatorRegistry

    udf_helpers = UDFHelpers()
    udfs = stdlib_register_udfs()
    udf_registry = UDFRegistry.with_udfs(*udfs)

    validators = stdlib_register_validators()
    registry = ValidatorRegistry.get_instance()
    for validator in validators:
        registry.register_to_instance(validator)

    sources_provider = get_sources_provider(rules_path=rules_path)

    engine = OspreyEngine(
        sources_provider=sources_provider,
        udf_registry=udf_registry,
        should_yield_during_compilation=should_yield_during_compilation(),
    )

    return engine, udf_helpers


class AsyncFileInputStream(AsyncBaseInputStream):
    """Read actions from a JSON file. Each line is a JSON action object."""

    def __init__(self, path: str):
        self._path = path

    async def _gen(self):
        with open(self._path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                action = Action(
                    action_id=data.get('id', 0),
                    action_name=data.get('name', 'unknown'),
                    data=data.get('data', {}),
                    timestamp=datetime.now(timezone.utc),
                )
                yield NoopAckingContext(action)


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.option('--rules-path', type=click.Path(exists=True), required=True, help='Path to rules directory')
@click.option('--input-file', type=click.Path(exists=True), default=None, help='Path to JSONL input file')
@click.option('--max-concurrent', type=int, default=12, help='Max concurrent async UDF executions')
@click.option('--with-plugins', is_flag=True, default=False, help='Load all plugins (requires external services)')
def run(rules_path: str, input_file: Optional[str], max_concurrent: int, with_plugins: bool) -> None:
    """Run the async rules worker with a static rules file and optional input file."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    logger.info('Starting async osprey worker (Phase 0)')
    logger.info(f'Rules path: {rules_path}')
    logger.info(f'Max concurrent UDFs: {max_concurrent}')

    config = init_config()

    if with_plugins:
        from osprey.worker.lib.osprey_engine import bootstrap_engine_with_helpers

        sources_provider = get_sources_provider(rules_path=rules_path)
        engine, udf_helpers = bootstrap_engine_with_helpers(sources_provider=sources_provider)
    else:
        engine, udf_helpers = bootstrap_stdlib_engine(rules_path)

    # Input stream
    if input_file:
        input_stream = AsyncFileInputStream(input_file)
    else:
        # No input — just validate the worker boots correctly
        input_stream = AsyncStaticInputStream([])

    # Output sink
    output_sink = AsyncStdoutOutputSink()

    # Build and run the async rules sink
    rules_sink = AsyncRulesSink(
        engine=engine,
        input_stream=input_stream,
        output_sink=output_sink,
        udf_helpers=udf_helpers,
        max_concurrent_udfs=max_concurrent,
    )

    async def _run():
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def _signal_handler():
            logger.info('Received shutdown signal')
            stop_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _signal_handler)

        sink_task = asyncio.create_task(rules_sink.run())

        # Wait for either the sink to finish or a shutdown signal
        done = asyncio.create_task(stop_event.wait())
        await asyncio.wait([sink_task, done], return_when=asyncio.FIRST_COMPLETED)

        if not sink_task.done():
            sink_task.cancel()
            try:
                await sink_task
            except asyncio.CancelledError:
                pass

        await rules_sink.stop()
        logger.info('Async worker shutdown complete')

    asyncio.run(_run())


@cli.command()
@click.option('--rules-path', type=click.Path(exists=True), required=True, help='Path to rules directory')
@click.option('--input-file', type=click.Path(exists=True), required=True, help='Path to JSONL input file')
@click.option('--max-concurrent', type=int, default=12, help='Max concurrent async UDF executions')
@click.option('--iterations', type=int, default=100, help='Number of iterations to run')
def benchmark(rules_path: str, input_file: str, max_concurrent: int, iterations: int) -> None:
    """Benchmark the async executor against the gevent executor."""
    import time

    logging.basicConfig(level=logging.WARNING)
    config = init_config()

    engine, udf_helpers = bootstrap_stdlib_engine(rules_path)

    # Load actions
    actions = []
    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            actions.append(
                Action(
                    action_id=data.get('id', 0),
                    action_name=data.get('name', 'unknown'),
                    data=data.get('data', {}),
                    timestamp=datetime.now(timezone.utc),
                )
            )

    if not actions:
        click.echo('No actions found in input file')
        return

    click.echo(f'Loaded {len(actions)} actions')
    click.echo(f'Running {iterations} iterations with max_concurrent={max_concurrent}')

    from osprey.async_worker.executor import execute as async_execute

    async def run_async_benchmark():
        start = time.perf_counter()
        for i in range(iterations):
            action = actions[i % len(actions)]
            await async_execute(
                engine.execution_graph,
                udf_helpers,
                action,
                max_concurrent=max_concurrent,
            )
        elapsed = time.perf_counter() - start
        return elapsed

    elapsed = asyncio.run(run_async_benchmark())
    throughput = iterations / elapsed
    avg_latency_ms = (elapsed / iterations) * 1000

    click.echo(f'\nAsync Executor Results:')
    click.echo(f'  Total time: {elapsed:.3f}s')
    click.echo(f'  Throughput: {throughput:.1f} actions/sec')
    click.echo(f'  Avg latency: {avg_latency_ms:.2f}ms')


if __name__ == '__main__':
    cli()
