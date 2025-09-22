# mypy: ignore-errors
# ruff: noqa: E402, E501
from osprey.worker.lib.patcher import patch_all
from osprey.worker.sinks.input_stream_chooser import get_rules_sink_input_stream
from osprey.worker.sinks.sink.output_sink import EventEffectsOutputSink

patch_all(ddtrace_args={'cassandra': True, 'psycopg': True})


import signal
from uuid import uuid1

# this is required to avoid memory leaks with gRPC
from gevent import config as gevent_config
from osprey.worker.adaptor.plugin_manager import bootstrap_ast_validators, bootstrap_output_sinks, bootstrap_udfs

gevent_config.track_greenlet_tree = False

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Optional, Set, TextIO, cast

import click
import gevent
import kafka
import sentry_sdk
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1
from osprey.worker.lib import instruments
from osprey.worker.lib.osprey_logging import configure_logging
from osprey.worker.lib.profiling.sampler import run_sampler_with_params

configure_logging()

from osprey.worker.lib.bulk_label import TaskStatus
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_engine import OspreyEngine, get_sources_provider
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.publisher import PubSubPublisher
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.sources_provider import EtcdSourcesProvider
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


@cli.command()
@click.option(
    '--rules-path',
    type=click.Path(dir_okay=True, file_okay=False, exists=True),
    help='Which rules to use. If not provided uses the rules in etcd.',
)
@click.option('--pooled/--no-pooled', default=True, help='Whether to run multiple sinks simultaneously in a pool.')
@click.option(
    '--bootstrap-pubsub/--no-bootstrap-pubsub',
    'bootstrap_pubsub',
    default=False,
    help='Create topics and subscriptions',
)
@click.option(
    '--bootstrap-bigtable/--no-bootstrap-bigtable',
    'bootstrap_bigtable',
    default=False,
    help='Create base tables',
)
def run_rules_sink(
    rules_path: Optional[str],
    pooled: bool,
    bootstrap_pubsub: bool,
    bootstrap_bigtable: bool,
) -> None:
    if bootstrap_pubsub:
        _bootstrap_pubsub()

    if bootstrap_bigtable:
        _bootstrap_bigtable()

    config = init_config()
    sources_provider = get_sources_provider(Path(rules_path) if rules_path is not None else None)

    udf_registry, udf_helpers = bootstrap_udfs()
    bootstrap_ast_validators()

    engine = OspreyEngine(sources_provider=sources_provider, udf_registry=udf_registry)

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
    udf_registry, udf_helpers = bootstrap_udfs()
    bootstrap_ast_validators()

    engine = OspreyEngine(sources_provider=sources_provider, udf_registry=udf_registry)

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

    sources_provider = EtcdSourcesProvider(
        etcd_key=config.get_str('OSPREY_ETCD_SOURCES_PROVIDER_KEY', '/config/osprey/rules-sink-sources')
    )
    udf_registry, _ = bootstrap_udfs()
    bootstrap_ast_validators()

    engine = OspreyEngine(sources_provider, udf_registry)

    analytics_pubsub_project_id = config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev')
    analytics_pubsub_topic_id = config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics')
    analytics_publisher = PubSubPublisher(analytics_pubsub_project_id, analytics_pubsub_topic_id)

    osprey_webhook_pubsub_project = config.get_str('PUBSUB_OSPREY_WEBHOOKS_PROJECT_ID', 'osprey-dev')
    osprey_webhook_pubsub_topic = config.get_str('PUBSUB_OSPREY_WEBHOOKS_TOPIC_ID', 'osprey-webhooks')
    webhooks_publisher = PubSubPublisher(osprey_webhook_pubsub_project, osprey_webhook_pubsub_topic)

    event_effects_output_sink = EventEffectsOutputSink(engine, analytics_publisher, webhooks_publisher)

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
@click.pass_context
@click.argument('task_id', type=int)
@click.option('--include-ids-from-file', type=click.File('r'))
def rollback_bulk_label_effects(
    ctx: click.Context, task_id: int, include_ids_from_file: Optional[TextIO] = None
) -> None:
    # TODO: Clean up this copy pasta.
    config = init_config()
    sources_provider = EtcdSourcesProvider(
        etcd_key=config.get_str('OSPREY_ETCD_SOURCES_PROVIDER_KEY', '/config/osprey/rules-sink-sources')
    )
    postgres.init_from_config('osprey_db')
    udf_registry, _ = bootstrap_udfs()
    bootstrap_ast_validators()

    engine = OspreyEngine(sources_provider, udf_registry)

    analytics_pubsub_project_id = config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev')
    analytics_pubsub_topic_id = config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics')
    analytics_publisher = PubSubPublisher(analytics_pubsub_project_id, analytics_pubsub_topic_id)

    osprey_webhook_pubsub_project = config.get_str('PUBSUB_OSPREY_WEBHOOKS_PROJECT_ID', 'osprey-dev')
    osprey_webhook_pubsub_topic = config.get_str('PUBSUB_OSPREY_WEBHOOKS_TOPIC_ID', 'osprey-webhooks')
    webhooks_publisher = PubSubPublisher(osprey_webhook_pubsub_project, osprey_webhook_pubsub_topic)

    task = BulkLabelTask.get_one(task_id)
    if task is None:
        click.echo(f'Task with id {task_id} not found.')
        ctx.abort()

    task = cast(BulkLabelTask, task)

    assert isinstance(task.task_status, TaskStatus)

    if not task.task_status.is_final():
        click.echo(f'Task with id {task_id} is not in final status, status is {task.task_status!r}.')
        ctx.abort()

    include_ids: Optional[Set[str]] = None
    if include_ids_from_file is not None:
        include_ids = {line.strip() for line in include_ids_from_file}
        include_ids.discard('')

    try:
        BulkLabelSink.rollback_task_effects(engine, analytics_publisher, webhooks_publisher, task, include_ids)
    finally:
        analytics_publisher.stop()
        webhooks_publisher.stop()


def _bootstrap_pubsub() -> None:
    os.environ['PUBSUB_EMULATOR_HOST'] = '127.0.0.1:8085'

    def create_pubsub_topic_and_subscription(
        topic_project_id: str, topic_id: str, subscription_project_id: str, subscription_id: str
    ) -> None:
        _create_pubsub_topic_and_subscription(
            publisher, subscriber, topic_project_id, topic_id, subscription_project_id, subscription_id
        )

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    create_pubsub_topic_and_subscription('osprey-dev', 'osprey-actions', 'osprey-dev', 'rules-sink')


def _create_pubsub_topic_and_subscription(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic_project_id: str,
    topic_id: str,
    subscription_project_id: str,
    subscription_id: str,
) -> None:
    topic_path = publisher.topic_path(topic_project_id, topic_id)
    try:
        publisher.create_topic(request={'name': topic_path})
        LOGGER.info(f'topic {topic_path} created')
    except AlreadyExists:
        LOGGER.error(f'topic {topic_path} already exists')

    subscription_path = subscriber.subscription_path(subscription_project_id, subscription_id)
    try:
        subscriber.create_subscription(request={'name': subscription_path, 'topic': topic_path})
        LOGGER.error(f'sub {subscription_path} created')
    except AlreadyExists:
        LOGGER.error(f'sub {subscription_path} already exists')


@cli.command()
def bootstrap_bigtable() -> None:
    init_config()
    _bootstrap_bigtable()


def _bootstrap_bigtable() -> None:
    os.environ['BIGTABLE_EMULATOR_HOST'] = '127.0.0.1:8361'
    osprey_bigtable.bootstrap()


@cli.command()
def bootstrap_pubsub() -> None:
    init_config()
    _bootstrap_pubsub()


@cli.command()
def start_shell():
    import IPython

    IPython.embed()


if __name__ == '__main__':
    # this is required for the multiprocessing to work
    multiprocessing.set_start_method('spawn', force=True)

    cli()
