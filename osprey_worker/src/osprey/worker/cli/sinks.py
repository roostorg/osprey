# mypy: ignore-errors
# ruff: noqa: E402, E501
from osprey.worker.lib.patcher import patch_all
from osprey.worker.sinks.sink.output_sink import EventEffectsOutputSink

patch_all(ddtrace_args={'cassandra': True, 'psycopg': True})


import signal
import time
from uuid import uuid1

# this is required to avoid memory leaks with gRPC
from gevent import config as gevent_config
from osprey.worker.adaptor.plugin_manager import bootstrap_ast_validators, bootstrap_output_sinks, bootstrap_udfs

gevent_config.track_greenlet_tree = False

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Optional, Set, TextIO, cast

import click
import gevent
import kafka
import msgpack
import sentry_sdk
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1
from osprey.worker.lib import instruments
from osprey.worker.lib.osprey_logging import configure_logging
from osprey.worker.lib.profiling.sampler import run_sampler_with_params

configure_logging()

from osprey.worker.lib.bulk_label import TaskStatus
from osprey.worker.lib.config import Config
from osprey.worker.lib.encryption.envelope import Envelope
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
    get_rules_sink_input_stream,
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
        input_stream_source = InputStreamSource(input_stream_source_string)
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
    create_pubsub_topic_and_subscription('osprey-dev', 'osprey-actions', 'osprey-dev', 'attachments-sent')
    create_pubsub_topic_and_subscription('osprey-dev', 'media-hasher-output', 'osprey-dev', 'media-match-sink')
    create_pubsub_topic_and_subscription('osprey-dev', 'osprey-analytics', 'osprey-dev', 'osprey-analytics-sub')
    create_pubsub_topic_and_subscription('osprey-dev', 'media-uploaded', 'osprey-dev', 'media-uploaded')
    create_pubsub_topic_and_subscription('osprey-dev', 'osprey-safety-signals', 'osprey-dev', 'safety-dispatch')
    create_pubsub_topic_and_subscription('osprey-dev', 'ihd-agent-decisions', 'osprey-dev', 'ihd-agent-decisions-sub')
    create_pubsub_topic_and_subscription('osprey-dev', 'osprey-evidence', 'osprey-dev', 'osprey-evidence-subscription')
    create_pubsub_topic_and_subscription('osprey-dev', 'osprey-webhooks', 'osprey-dev', 'osprey-webhooks-subscription')
    create_pubsub_topic_and_subscription('osprey-dev', 'panther-osprey-events', 'osprey-dev', 'panther-osprey-events')


# NOTE(austin) in reality this shouldn't live in the osprey cli but the infrastructure already exists
def _bootstrap_safety_record_pubsub() -> None:
    os.environ['PUBSUB_EMULATOR_HOST'] = '127.0.0.1:8085'

    def create_pubsub_topic_and_subscription(
        topic_project_id: str, topic_id: str, subscription_project_id: str, subscription_id: str
    ) -> None:
        _create_pubsub_topic_and_subscription(
            publisher, subscriber, topic_project_id, topic_id, subscription_project_id, subscription_id
        )

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    create_pubsub_topic_and_subscription(
        'osprey-dev', 'safety-record-write-topic', 'osprey-dev', 'safety-record-write-topic-subscription'
    )
    create_pubsub_topic_and_subscription(
        'osprey-dev', 'safety-classification-emit-topic', 'osprey-dev', 'safety-classification-emit-subscription'
    )
    create_pubsub_topic_and_subscription(
        'osprey-dev',
        'safety-classification-removal-emit-topic',
        'osprey-dev',
        'safety-classification-removal-emit-subscription',
    )

    create_pubsub_topic_and_subscription(
        'osprey-dev', 'interventions-request', 'osprey-dev', 'interventions-request-subscription'
    )
    create_pubsub_topic_and_subscription(
        'osprey-dev',
        'safety-record-evidence-write-topic',
        'osprey-dev',
        'safety-record-evidence-write-topic-subscription',
    )
    create_pubsub_topic_and_subscription(
        'osprey-dev',
        'safety-record-evidence-write-dead-letter-topic',
        'osprey-dev',
        'safety-record-evidence-write-dead-letter-topic-subscription',
    )
    create_pubsub_topic_and_subscription('osprey-dev', 'generic-events', 'osprey-dev', 'generic-events-subscription')

    create_pubsub_topic_and_subscription(
        'osprey-dev',
        'safety-classification-removal-requests',
        'osprey-dev',
        'safety-classification-removal-requests-subscriber',
    )

    create_pubsub_topic_and_subscription(
        'osprey-dev',
        'interventions-request',
        'osprey-dev',
        'interventions-request-subscription',
    )
    create_pubsub_topic_and_subscription(
        'osprey-dev',
        'interventions-flush',
        'osprey-dev',
        'interventions-flush-subscription',
    )
    create_pubsub_topic_and_subscription(
        'osprey-dev',
        'safety-classification-blast-radius-review',
        'osprey-dev',
        'safety-classification-blast-radius-review-subscription',
    )


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
@click.option(
    '--sleep-forever/--no-sleep-forever',
    default=False,
    help='Whether to sleep forever after bootstrapping the pubsub topics/subscriptions. '
    'Intended for clyde service start so that we can do one clean deployment.',
)
def bootstrap_safety_record_pubsub(sleep_forever: bool) -> None:
    _bootstrap_safety_record_pubsub()
    if sleep_forever:
        while True:
            time.sleep(1)


unencrypted_action_example_json = {
    'name': 'guild_joined',
    'data': {
        'user': {
            'id': '1',
            'username': 'RyanWeinstein',
            'avatar': 'a_9e573467f84ae50d47ffb69f4b11e4cb',
            'avatar_decoration': None,
            'discriminator': '0001',
            'public_flags': 129,
            'mfa_enabled': True,
            'bot': False,
            'flags': 10857677324417,
            'email_verified': True,
            'required_action': None,
            'premium_type': 2,
            'premium_until': '2022-05-03T21:49:26.832000+00:00',
            'phone': '+14045528784',
            'email': 'rnw144@gmail.com',
            'ip': '76.21.5.241',
            'ip_is_whitelisted': False,
        },
        'invite': {
            'code': '4ucRCcAV',
            'max_age': 604800,
            'created_at': '2022-04-28T19:55:05.704694+00:00',
            'temporary': False,
            'max_uses': 0,
            'custom': False,
        },
        'guild': {
            'id': '1',
            'name': 'SF Go Club',
            'description': None,
            'owner': {'id': '1'},
            'member_count': 113,
            'features': ['EXPOSED_TO_ACTIVITIES_WTP_EXPERIMENT'],
            'premium_subscription_count': 0,
            'verification_level': 1,
            'is_nsfw': False,
            'nsfw_level': 0,
            'has_public_stage': False,
            'channel_names': [],
        },
        'channel': {'id': '1', 'name': 'general', 'type': 0, 'is_nsfw': False},
        'inviter': {
            'id': '1',
            'username': 'RyanWeinstein',
            'avatar': 'a_9e573467f84ae50d47ffb69f4b11e4cb',
            'avatar_decoration': None,
            'discriminator': '0001',
            'public_flags': 129,
            'mfa_enabled': True,
            'bot': False,
            'flags': 10857677324417,
            'email_verified': True,
            'required_action': None,
            'premium_type': 2,
            'premium_until': '2022-05-03T21:49:26.832000+00:00',
            'phone': '+14045528784',
            'email': 'rnw144@gmail.com',
            'ip': '76.21.5.241',
            'ip_is_whitelisted': False,
        },
        'properties': {
            'os': 'Mac OS X',
            'browser': 'Osprey Client',
            'release_channel': 'canary',
            'client_version': '0.0.284',
            'os_version': '20.6.0',
            'os_arch': 'x64',
            'system_locale': 'en-US',
            'client_build_number': 124425,
            'client_event_source': None,
            'location': 'Guild Events',
            'cfduid': 'bb13eb5bf3ea11ebb95442010a0a0015',
            'cfduid_signed': True,
        },
        'http_request': {
            'headers': {
                'Host': 'test.example.com',
                'X-Forwarded-For': '76.21.5.241',
                'X-Forwarded-Proto': 'https',
                'Content-Length': '84',
                'Accept-Encoding': 'gzip',
                'Cf-Ipcountry': 'US',
                'Cf-Ray': '70325a8013769858-SJC',
                'Cf-Visitor': '{"scheme":"https"}',
                'Content-Type': 'application/json',
                'Cf-Ew-Via': '15',
                'Cdn-Loop': 'cloudflare; subreqs=1',
                'Accept-Language': 'en-US',
                'Accept': '*/*',
                'Referer': 'https://text.example.com/channels/1/1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) osprey/0.0.284 Chrome/91.0.4472.164 Electron/13.6.6 Safari/537.36',
                'Cf-Connecting-Ip': '76.21.5.241',
                'Origin': 'https://test.example.com',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'X-Context-Properties': 'eyJsb2NhdGlvbiI6Ikd1aWxkIEV2ZW50cyJ9',
                'X-Debug-Options': 'bugReporterEnabled',
                'X-Super-Properties': 'eyJvcyI6Ik1hYyBPUyBYIiwiYnJvd3NlciI6IkRpc2NvcmQgQ2xpZW50IiwicmVsZWFzZV9jaGFubmVsIjoiY2FuYXJ5IiwiY2xpZW50X3ZlcnNpb24iOiIwLjAuMjg0Iiwib3NfdmVyc2lvbiI6IjIwLjYuMCIsIm9zX2FyY2giOiJ4NjQiLCJzeXN0ZW1fbG9jYWxlIjoiZW4tVVMiLCJjbGllbnRfYnVpbGRfbnVtYmVyIjoxMjQ0MjUsImNsaWVudF9ldmVudF9zb3VyY2UiOm51bGx9',
                'Cf-Worker': 'example.com',
                'X-Cloud-Trace-Context': '26c610f0ab9788509722bd55f34e480c/4736332299159169251',
                'Cookie': '__dcfduid=5ad2d58a06e14efa95adb3ff566ae6d5; __sdcfduid=bb13eb5bf3ea11ebb95442010a0a00151f59c766ecd2e78b5037913f45cf34e462ebde0db80fdcceb2e479c04e633b3a; __stripe_mid=d0fa98c9-ea86-43c0-93d9-e65470664ea24b054b; __cfruid=85af4fba395350750926aa70ffe390723c78fccc-1650303703',
                'Via': '1.1 google',
                'X-Envoy-External-Address': '10.10.3.166',
                'X-Request-Id': 'cd8d1e03-0763-42c7-83b2-d1f42d6a70a0',
                'X-Datadog-Trace-Id': '5264020418323037859',
                'X-Datadog-Parent-Id': '7050569386306678149',
                'X-Datadog-Sampling-Priority': '-1',
            },
            'endpoint': 'invites.create_channel_invite',
            'url': 'https://test.example.com/api/channels/900896022295871500/invites',
            'query_string': '',
            'method': 'POST',
            'api_version': 9,
            'remote_addr': '76.21.5.241',
            'remote_addr_is_whitelisted': False,
        },
        'did_solve_captcha': False,
    },
}

encrypted_action_example_json = {
    'name': 'message_sent',
    'secret_data': {
        'message': {
            'content': 'This should be encrypted message content!',
        },
    },
    'data': {
        'channel': {
            'id': '1',
            'name': None,
            'type': 1,
            'is_nsfw': False,
            'message_count': 9,
            'recipients': [{'id': '1'}],
        },
        'message': {
            'id': '1/1',
            'message_id': '1',
            'channel_id': '1',
            'edited_timestamp': None,
            'flags': 0,
            'mention_ids': [],
            'mention_channel_ids': [],
            'mention_everyone': None,
            'mention_everyone_online_only': None,
            'author': {
                'id': '1',
                'username': 'person_a',
                'global_name': 'person_a',
                'avatar': '393729487298e9e7b98739287923d927',
                'discriminator': '0',
                'public_flags': 64,
                'bio': 'owo',
                'mfa_enabled': True,
                'avatar_decoration': None,
                'bot': False,
                'flags': 10445360463936,
                'suspicious_activity_flags': 0,
                'email_verified': True,
                'required_action': None,
                'premium_type': None,
                'premium_until': None,
                'is_unique_username': True,
                'phone': '+1112223333',
                'email': 'person_a@example.com',
                'ip': '127.0.0.1',
                'ip_is_whitelisted': False,
            },
        },
        'user': {
            'id': '1',
            'username': 'person_a',
            'global_name': 'person_a',
            'avatar': '393729487298e9e7b98739287923d927',
            'discriminator': '0',
            'public_flags': 64,
            'bio': 'owo',
            'mfa_enabled': True,
            'avatar_decoration': None,
            'bot': False,
            'flags': 10445360463936,
            'suspicious_activity_flags': 0,
            'email_verified': True,
            'required_action': None,
            'premium_type': None,
            'premium_until': None,
            'is_unique_username': True,
            'phone': '+1112223333',
            'email': 'person_a@example.com',
            'ip': '127.0.0.1',
            'ip_is_whitelisted': False,
        },
    },
}


@cli.command()
@click.argument('count', type=int, default=1)
@click.argument('delay', type=float, default=0.0)
@click.option('--encrypted/--unencrypted', 'encrypted', default=False, help='Whether to encrypt the action')
def publish_action(count: int, delay: float, encrypted: bool) -> None:
    os.environ['PUBSUB_EMULATOR_HOST'] = 'localhost:8085'
    import json
    import time

    _bootstrap_pubsub()
    publisher = pubsub_v1.PublisherClient()
    envelope: Optional[Envelope] = None

    if encrypted:
        config = init_config()
        envelope = Envelope(
            kek_uri=config.get_str('PUBSUB_ENCRYPTION_KEY_URI', ''),
            gcp_credential_path='',
        )

    topic_path = publisher.topic_path('osprey-dev', 'osprey-actions')
    for i in range(count):
        # coordinator should generate the snowflake id
        action: Dict[str, Any] = {}
        # action: Dict[str, Any] = {'id': str(round(datetime.now().timestamp()) + i)}
        if encrypted:
            action.update({**encrypted_action_example_json})
        else:
            action.update({**unencrypted_action_example_json})
        attributes = {}
        data = msgpack.dumps(json.dumps(action))
        if encrypted and envelope is not None:
            attributes['encrypted'] = 'true'
            data = envelope.encrypt(data)
        else:
            attributes['encrypted'] = 'false'
        future = publisher.publish(topic_path, data, **attributes)
        print('Published message with id:', future.result())
        time.sleep(delay)


@cli.command()
def start_shell():
    import IPython

    IPython.embed()


if __name__ == '__main__':
    # this is required for the multiprocessing to work
    multiprocessing.set_start_method('spawn', force=True)

    cli()
