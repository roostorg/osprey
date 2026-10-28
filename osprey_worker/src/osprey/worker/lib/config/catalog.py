"""Catalog of the configuration keys Osprey reads, for `osprey-cli config dump`.

Osprey's Python config is the process environment (see `config_from_env`), read through `Config.get_*` /
`expect_*` and `os.environ` at call sites across the codebase. The dump only reports keys listed here, never the
raw environment, so a new key must be added here to show up. `tests/test_config_catalog.py` scans the source and
fails when a key read in code is missing.

`default` is the value the code falls back to when the key is unset (`None`: no default; the key is optional or
required). `derived_default` computes a default from the environment, for defaults that aren't literals.
`sensitive` overrides the key-name redaction rules in `redaction.py`: `True` always redacts, `False` marks a key
whose name only looks secret (value-shape rules still apply). A key read by several components with different
defaults (e.g. the coordinator and the worker) has one entry per component.
"""

import platform
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

COMMON = 'common'
WORKER = 'worker'
UI_API = 'ui_api'
ASYNC_WORKER = 'async_worker'
COORDINATOR = 'coordinator'
UI = 'ui'

# Components whose config is the environment of the Python process running the dump.
PYTHON_COMPONENTS = (COMMON, WORKER, UI_API, ASYNC_WORKER)
# Other processes' config; dump these by passing that process's environment with `--env-file`.
ALL_COMPONENTS = (*PYTHON_COMPONENTS, COORDINATOR, UI)


@dataclass(frozen=True)
class ConfigKey:
    name: str
    components: tuple[str, ...]
    default: Any = None
    derived_default: Callable[[Mapping[str, str]], Any] | None = field(default=None, compare=False)
    sensitive: bool | None = None


def _key(name: str, *components: str, **kwargs: Any) -> ConfigKey:
    return ConfigKey(name=name, components=components, **kwargs)


CATALOG: tuple[ConfigKey, ...] = (
    # Shared libraries: config machinery, logging, tracing, storage, etcd.
    _key('DEBUG', COMMON, default=False),
    _key('TESTING', COMMON, default=False),
    _key('ENVIRONMENT', COMMON),
    _key('OSPREY_COMMIT', COMMON),  # reported as app.commit
    _key('BASE_URL', COMMON, default='https://localhost'),
    _key('OSPREY_PYTHON_LOG_HANDLER', COMMON),
    _key('OSPREY_DD_LOGGING', COMMON, default=True),
    _key('TRACING_CONFIGURE_DATADOG', COMMON, default=False),
    _key('TRACING_COLLECT_DETAILED_GC', COMMON, default=False),
    _key('DATADOG_PREFIX', COMMON, default='osprey'),
    _key('DD_SERVICE', COMMON),
    _key('DD_ENV', COMMON),
    _key('DD_VERSION', COMMON),
    _key('DD_AGENT_HOST', COMMON, default='localhost'),
    _key('DD_AGENT_PORT', COMMON, default=8125),
    _key('DD_TRACE_ENABLED', COMMON),
    _key('DD_DOGSTATSD_DISABLE', COMMON),
    _key('DOGSTATSD_SOCKET_PATH', COMMON),
    _key('PYROSCOPE_HOST', COMMON, default='localhost'),
    _key('PYROSCOPE_PORT', COMMON, default='4040'),
    _key(
        'PYROSCOPE_URL',
        COMMON,
        derived_default=lambda env: (
            f'http://{env.get("PYROSCOPE_HOST", "localhost")}:{env.get("PYROSCOPE_PORT", "4040")}/ingest'
        ),
    ),
    _key('POSTGRES_HOSTS', COMMON),
    _key('ETCD_PEERS', COMMON, default='http://127.0.0.1:2379'),
    _key('ETCD_PEERS_WATCHERD_ADDR', COMMON),
    _key('ETCD_SECURE_PEERS', COMMON, default='http://127.0.0.1:2379'),
    _key('ETCD_SECURE_PEERS_WATCHERD_ADDR', COMMON),
    _key('SNOWFLAKE_API_ENDPOINT', COMMON),
    _key('SNOWFLAKE_EPOCH', COMMON, default=0),
    _key('OSPREY_RULES_PATH', COMMON),
    _key('OSPREY_ETCD_SOURCES_PROVIDER_KEY', COMMON, default='/config/osprey/rules-sink-sources', sensitive=False),
    _key('OSPREY_PERIODIC_YIELD_DURING_COMPILATION', COMMON, default=False),
    _key('OSPREY_PUBSUB_ENABLED', COMMON, default=False),
    _key('OSPREY_DISABLE_VALIDATION_EXPORTER', COMMON, default=False),
    _key('PUBSUB_DATA_PROJECT_ID', COMMON, default='osprey-dev'),
    _key('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', COMMON, default='osprey-analytics'),
    _key('PUBSUB_EMULATOR_HOST', COMMON),
    _key('BIGTABLE_EMULATOR_HOST', COMMON),
    _key('GOOGLE_APPLICATION_CREDENTIALS', COMMON),
    _key('OSPREY_GCP_PROJECT_ID', COMMON, default='osprey-dev'),
    _key('OSPREY_BIGTABLE_INSTANCE_ID', COMMON, default='osprey-bigtable'),
    _key('OSPREY_BIGTABLE_ADMIN_ENABLED', COMMON, default=True),
    _key('OSPREY_EXECUTION_RESULT_STORAGE_BACKEND', COMMON, default='none'),
    _key('OSPREY_GCS_EXECUTION_RESULTS_BUCKET', COMMON, default='osprey-execution-results-stg'),
    _key('OSPREY_MINIO_ENDPOINT', COMMON, default='minio:9000'),
    _key('OSPREY_MINIO_ACCESS_KEY', COMMON, default='minioadmin'),
    _key('OSPREY_MINIO_SECRET_KEY', COMMON, default='minioadmin123'),
    _key('OSPREY_MINIO_SECURE', COMMON, default=False),
    _key('OSPREY_MINIO_EXECUTION_RESULTS_BUCKET', COMMON, default='execution-output'),
    _key('OSPREY_BULK_ACTION_FILE_UPLOAD_BUCKET', COMMON, default='osprey-bulk-action-file-upload-stg'),
    # Rules worker (`osprey-cli` sinks, input streams, output sinks).
    _key('PORT', WORKER, UI_API),
    _key('RUN_PROFILER', WORKER, default='false'),
    _key('SENTRY_RULES_SINK_DSN', WORKER, default=''),
    _key('SENTRY_OTHER_SINKS_DSN', WORKER, default=''),
    _key('OSPREY_INPUT_STREAM_SOURCE', WORKER, default='plugin'),
    _key('OSPREY_RULES_SINK_NUM_WORKERS', WORKER),
    _key('OSPREY_BULK_LABEL_SINK_NUM_WORKERS', WORKER),
    _key('OSPREY_RULES_WORKER_PROCESS_COUNT', WORKER, default=1),
    _key('OSPREY_COORDINATOR_SERVICE_NAME', WORKER, COORDINATOR, default='osprey_coordinator'),
    _key('OSPREY_KAFKA_BOOTSTRAP_SERVERS', WORKER, default=['localhost']),
    _key('OSPREY_KAFKA_INPUT_STREAM_TOPIC', WORKER, COORDINATOR, default='osprey.actions_input'),
    _key('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID', WORKER, derived_default=lambda env: platform.node()),
    _key('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID_SUFFIX', WORKER),
    _key('OSPREY_KAFKA_GROUP_ID', WORKER),
    _key('OSPREY_KAFKA_OUTPUT_SINK', WORKER, default=False),
    _key('OSPREY_KAFKA_OUTPUT_TOPIC', WORKER),
    _key('OSPREY_KAFKA_OUTPUT_CLIENT_ID', WORKER),
    _key('OSPREY_KAFKA_OUTPUT_SINK_TOPIC', WORKER, default='osprey.execution_results'),
    _key('OSPREY_STDOUT_OUTPUT_SINK', WORKER, default=False),
    _key('PUBSUB_OSPREY_PROJECT_ID', WORKER, default='osprey-dev'),
    _key('PUBSUB_OSPREY_RULES_SINK_SUBSCRIPTION', WORKER, default='rules-sink'),
    _key('PUBSUB_OSPREY_INPUT_BATCH_SIZE', WORKER, default=100),
    _key('PUBSUB_OSPREY_INPUT_STREAM_GEVENT_QUEUE_SIZE', WORKER, default=1000),
    # A Cloud KMS key resource URI, not key material.
    _key('PUBSUB_ENCRYPTION_KEY_URI', WORKER, COORDINATOR, default='', sensitive=False),
    # GCP project and topic names; "webhooks" in the name doesn't make them secret.
    _key('PUBSUB_OSPREY_WEBHOOKS_PROJECT_ID', WORKER, default='osprey-dev', sensitive=False),
    _key('PUBSUB_OSPREY_WEBHOOKS_TOPIC_ID', WORKER, default='osprey-webhooks', sensitive=False),
    # UI API (Flask).
    _key('FLASK_DEBUG', UI_API),
    _key('FLASK_ENV', UI_API),
    _key('SENTRY_UI_API_DSN', UI_API, default=''),
    _key('DRUID_URL', UI_API),
    _key('DRUID_DATASOURCE', UI_API, default='osprey.execution_results'),
    _key('MAX_HISTORICAL_QUERY_WINDOW_DAYS', UI_API, default=90),
    _key('OSPREY_DEV_USER_EMAIL', UI_API, default='local-dev@localhost'),
    # Experimental asyncio worker.
    _key('OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT', ASYNC_WORKER, default=2.0),
    _key('OSPREY_MAX_ASYNC_PER_EXECUTION', ASYNC_WORKER, default=12),
    # Coordinator (Rust; env vars and the clap flags they back).
    _key('OSPREY_COORDINATOR_BIDI_STREAM_PORT', COORDINATOR, default=19950),
    _key('OSPREY_COORDINATOR_SYNC_ACTION_PORT', COORDINATOR, default=19951),
    _key('OSPREY_COORDINATOR_CONSUMER_TYPE', COORDINATOR),
    _key('OSPREY_COORDINATOR_SERVICE_ACCOUNT', COORDINATOR, default='default'),
    _key('SNOWFLAKE_API_ENDPOINT', COORDINATOR, default='http://localhost:19952'),
    _key('OSPREY_KAFKA_BOOTSTRAP_SERVERS', COORDINATOR, default='localhost:9092'),
    _key('OSPREY_KAFKA_GROUP_ID', COORDINATOR, default='osprey_coordinator_group'),
    _key('ETCD_PEERS', COORDINATOR),
    _key('ETCD_LOCK', COORDINATOR),
    _key('ETCD_SECURE_PEERS', COORDINATOR),
    _key('GOOGLE_APPLICATION_CREDENTIALS', COORDINATOR),
    _key('PUBSUB_EMULATOR_HOST', COORDINATOR),
    _key('DD_AGENT_HOST', COORDINATOR, default='127.0.0.1'),
    _key('POD_IP', COORDINATOR),
    _key('DISCOVERY_HOSTNAME_SUFFIX', COORDINATOR),
    _key('MAX_PQ_RECEIVE_AWAIT_TIME_MS', COORDINATOR, default=5000),
    _key('MAX_TIME_TO_SEND_TO_ASYNC_QUEUE_MS', COORDINATOR, default=500),
    _key('MAX_ACKING_RECEIVER_WAIT_TIME_MS', COORDINATOR, default=60000),
    _key('DATADOG_TRACE_AGENT_HOSTNAME', COORDINATOR),
    _key('DD_ENTITY_ID', COORDINATOR),
    _key('CADENCE_DEFAULT_BUFFER_SIZE', COORDINATOR),
    _key('GRPC_DEFAULT_SSL_ROOTS_FILE_PATH', COORDINATOR),
    _key('ETCD_USERNAME', COORDINATOR),
    _key('ETCD_PASSWORD', COORDINATOR),
    _key('PUBSUB_CHANNEL_TIMEOUT_SECS', COORDINATOR, default=5),
    _key('PUBSUB_SUBSCRIPTION_PROJECT_ID', COORDINATOR, default='osprey-dev'),
    _key('PUBSUB_SUBSCRIPTION_ID', COORDINATOR, default='osprey-coordinator-actions'),
    _key('PUBSUB_MAX_MESSAGES', COORDINATOR, default=5000),
    _key('PUBSUB_MAX_PROCESSING_MESSAGES', COORDINATOR, default=5000),
    _key('PUBSUB_MIN_LEASE_EXTENSION_SECS', COORDINATOR, default=30),
    _key('PUBSUB_MAX_LEASE_EXTENSION_SECS', COORDINATOR, default=600),
    # UI (build-time; baked into the public JS bundle).
    _key('REACT_APP_API_BASE_URL', UI),
    _key('REACT_APP_SENTRY_DSN', UI),
)
