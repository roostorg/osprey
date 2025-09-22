# mypy: ignore-errors
# ruff: noqa: E402, E501
from osprey.worker.lib.patcher import patch_all
from osprey.worker.sinks.input_stream_chooser import get_rules_sink_input_stream
from osprey.worker.sinks.sink.output_sink import LabelEffectsOutputSink

patch_all(ddtrace_args={'cassandra': True, 'psycopg': True})


import signal
from uuid import uuid1

# this is required to avoid memory leaks with gRPC
from gevent import config as gevent_config
from osprey.worker.adaptor.plugin_manager import bootstrap_output_sinks

gevent_config.track_greenlet_tree = False

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor

import click
import gevent
import kafka
import sentry_sdk
from osprey.worker.lib import instruments
from osprey.worker.lib.osprey_logging import configure_logging
from osprey.worker.lib.profiling.sampler import run_sampler_with_params

configure_logging()

from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_engine import bootstrap_engine, bootstrap_engine_with_helpers, get_sources_provider
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.publisher import PubSubPublisher
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage import postgres
from osprey.worker.lib.storage.bigtable import osprey_bigtable
from osprey.worker.lib.storage.bulk_label_task import BulkLabelTask
from osprey.worker.lib.utils.input_stream_ready_signaler import InputStreamReadySignaler
from osprey.worker.sinks import (
    InputStreamSource,
)
from osprey.worker.sinks.sink.base_sink import BaseSink, PooledSink
from osprey.worker.sinks.sink.bulk_label_sink import BulkLabelSink
from osprey.worker.sinks.sink.input_stream import PostgresInputStream
from osprey.worker.sinks.sink.osprey_coordinator_input_stream import OspreyCoordinatorInputStream
from osprey.worker.sinks.sink.rules_sink import RulesSink
from osprey.worker.sinks.utils.kafka import PatchedKafkaConsumer

LOGGER = get_logger()

CONFIG_SENTRY_OTHER_SINKS_DSN = 'SENTRY_OTHER_SINKS_DSN'


def init_config() -> Config:
    config = CONFIG.instance()
    config.configure_from_env()
    return config


def gevent_liveliness_watcher() -> None:
    while True:
        with instruments.metrics.timed('gevent_tick_duration', use_ms=True, sample_rate=0.1):
            gevent.sleep(1 / 60.0)


@click.group()
def cli() -> None:
    pass


@cli.command()
def tail_kafka_output_sink() -> None:
    config = init_config()
    output_topic = config.get_str('OSPREY_KAFKA_OUTPUT_SINK_TOPIC', 'osprey.execution_results')
    bootstrap_servers = config.get_str_list('OSPREY_KAFKA_BOOTSTRAP_SERVERS', ['localhost'])

    kakfa_consumer = kafka.KafkaConsumer(output_topic, bootstrap_servers=bootstrap_servers)
    for event in kakfa_consumer:
        print(event)


@cli.command()
def tail_pubsub_input_sink() -> None:
    init_config()
    input_stream = get_rules_sink_input_stream(InputStreamSource.PUBSUB)
    for message_context in input_stream:
        with message_context as message:
            print(message)


@cli.command()
def tail_kafka_input_sink() -> None:
    config = init_config()
    client_id = config.get_str('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID', 'localhost')
    bootstrap_servers = config.get_str_list('OSPREY_KAFKA_BOOTSTRAP_SERVERS', ['localhost'])
    input_topic = config.get_str('OSPREY_KAFKA_INPUT_STREAM_TOPIC', 'osprey.actions_input')
    kafka_consumer = PatchedKafkaConsumer(input_topic, bootstrap_servers=bootstrap_servers, client_id=client_id)
    for event in kafka_consumer:
        print(event)


@click.option('--pooled/--no-pooled', default=True, help='Whether to run multiple sinks simultaneously in a pool.')
@click.option(
    '--bootstrap-bigtable/--no-bootstrap-bigtable',
    'bootstrap_bigtable',
    default=False,
    help='Create base tables',
)
def run_rules_sink(
    pooled: bool,
    bootstrap_bigtable: bool,
) -> None:
    if bootstrap_bigtable:
        _bootstrap_bigtable()

    config = init_config()
    engine, udf_helpers = bootstrap_engine_with_helpers()

    input_stream_source_string = config.get_str('OSPREY_INPUT_STREAM_SOURCE', 'plugin')
    try:
        input_stream_source = InputStreamSource(input_stream_source_string.lower())
    except ValueError:
        raise NotImplementedError(f'{input_stream_source_string} is not a valid input stream source.')

    input_stream = get_rules_sink_input_stream(input_stream_source)
    output_sink = bootstrap_output_sinks(config=config)

    def factory() -> BaseSink:
        return RulesSink(
            engine=engine,
            input_stream=input_stream,
            output_sink=output_sink,
            udf_helpers=udf_helpers,
        )

    if pooled:
        rules_sink: BaseSink = PooledSink(factory, num_workers=config.expect_int('OSPREY_RULES_SINK_NUM_WORKERS'))
    else:
        rules_sink = factory()

    gevent.spawn(gevent_liveliness_watcher)

    rules_sink.run()


def _run_rules_worker_process() -> None:
    """
    Internal helper to run a rules worker process
    """
    gevent.spawn(gevent_liveliness_watcher)

    if os.environ.get('RUN_PROFILER') == 'true':
        gevent.spawn(run_sampler_with_params, 1, 30)

    pid = os.getpid()
    config = CONFIG.instance()
    try:
        config.configure_from_env()  # needed when start_method for multiprocessing is spawn
    except RuntimeError:
        LOGGER.debug(
            f'{pid}: Config has already been bound to an underlying config dict. Proceeding without re-configuring.'
        )

    # Input Stream
    input_stream_ready_signaler = InputStreamReadySignaler()
    input_stream = OspreyCoordinatorInputStream(
        client_id=f'{uuid1()}', input_stream_ready_signaler=input_stream_ready_signaler
    )
    signal.signal(signal.SIGTERM, lambda *args: input_stream.stop())
    signal.signal(signal.SIGINT, lambda *args: input_stream.stop())

    # Sources and Engine
    sources_provider = get_sources_provider(rules_path=None, input_stream_ready_signaler=input_stream_ready_signaler)

    engine, udf_helpers = bootstrap_engine_with_helpers(sources_provider=sources_provider)

    # Output Sink
    output_sink = bootstrap_output_sinks(config)

    # Rules Worker
    rules_sink = RulesSink(
        engine=engine,
        input_stream=input_stream,
        output_sink=output_sink,
        udf_helpers=udf_helpers,
    )
    try:
        LOGGER.info(f'{pid}: Rules worker spawned')
        rules_sink.run()
    finally:
        # Make sure all output sinks shutdown down properly, what that means is up to each sink
        # TODO: Stopping the rules sink should also call stop on its output sink
        output_sink.stop()
        rules_sink.stop()


@cli.command()
def run_rules_worker_production() -> None:
    """Entrypoint for rules worker in production

    Input Stream: OspreyCoordinatorInputStream
    Output: Full set of output sinks, including but not limited to
        ExecutionResult, Sentry, Analytics, Panther, EventEffects, SafetyRecord

    TODO: this should just call run_rules_sink with the appropriate parameters
    """
    LOGGER.info('Setting up rules worker')
    config = init_config()
    num_processes = config.get_int('OSPREY_RULES_WORKER_PROCESS_COUNT', 1)

    if num_processes <= 1:
        LOGGER.info('OSPREY_RULES_WORKER_PROCESS_COUNT was not greater than 1, running single process rules worker')

        return _run_rules_worker_process()

    LOGGER.info(f'OSPREY_RULES_WORKER_PROCESS_COUNT was set to {num_processes}, running multi-process rules worker')

    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(_run_rules_worker_process) for _ in range(num_processes)]
        try:
            for future in futures:
                future.result()
        except KeyboardInterrupt:
            executor.shutdown(wait=True)
            raise


@cli.command()
@click.option('--pooled/--no-pooled', default=True, help='Whether to run multiple bulk label sinks in a pool')
@click.option(
    '--send-status-webhook/--no-send-status-webhook',
    default=True,
    help='Whether to send status webhook to channel specified in rules repo',
)
def run_bulk_label_sink(pooled: bool, send_status_webhook: bool) -> None:
    config = init_config()

    sentry_dsn = config.get_str(CONFIG_SENTRY_OTHER_SINKS_DSN, '')
    if sentry_dsn:
        sentry_sdk.init(sentry_dsn)

    postgres.init_from_config('osprey_db')

    engine = bootstrap_engine()

    analytics_pubsub_project_id = config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev')
    analytics_pubsub_topic_id = config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics')
    analytics_publisher = PubSubPublisher(analytics_pubsub_project_id, analytics_pubsub_topic_id)

    osprey_webhook_pubsub_project = config.get_str('PUBSUB_OSPREY_WEBHOOKS_PROJECT_ID', 'osprey-dev')
    osprey_webhook_pubsub_topic = config.get_str('PUBSUB_OSPREY_WEBHOOKS_TOPIC_ID', 'osprey-webhooks')
    webhooks_publisher = PubSubPublisher(osprey_webhook_pubsub_project, osprey_webhook_pubsub_topic)

    event_effects_output_sink = LabelEffectsOutputSink(engine, analytics_publisher, webhooks_publisher)

    def factory() -> BulkLabelSink:
        # NOTE: It's very important the input stream is created per-webhook sink
        postgres_source = PostgresInputStream(BulkLabelTask, tags=['sink:bulklabelsink'])
        return BulkLabelSink(
            input_stream=postgres_source,
            event_effects_output_sink=event_effects_output_sink,
            engine=engine,
            analytics_publisher=analytics_publisher,
            send_status_webhook=send_status_webhook,
        )

    if pooled:
        bulk_label_sink: BaseSink = PooledSink(
            factory, num_workers=config.expect_int('OSPREY_BULK_LABEL_SINK_NUM_WORKERS')
        )
    else:
        bulk_label_sink = factory()

    try:
        bulk_label_sink.run()
    finally:
        bulk_label_sink.stop()


@cli.command()
def bootstrap_bigtable() -> None:
    init_config()
    _bootstrap_bigtable()


def _bootstrap_bigtable() -> None:
    os.environ['BIGTABLE_EMULATOR_HOST'] = '127.0.0.1:8361'
    osprey_bigtable.bootstrap()


@cli.command()
def start_shell():
    import IPython

    IPython.embed()


if __name__ == '__main__':
    # this is required for the multiprocessing to work
    multiprocessing.set_start_method('spawn', force=True)

    cli()
