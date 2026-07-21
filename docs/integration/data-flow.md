# Data Flow

How events actually reach Osprey, how results come back out, and what to change (not fork) if your platform doesn't look like the default docker-compose setup.

## Architecture at a glance

At its core, Osprey is a pipeline: events come in, get evaluated against your SML rules, and verdicts/effects go out.

```
   Kafka topic(s) / PubSub / gRPC
                │
                ▼
   Osprey Coordinator (Rust, optional)
                │
                ▼
       Osprey Worker (Python)
     evaluates rules against the
      event, produces an
      ExecutionResult
                │
                ▼
   Output sink(s) + execution result store
   (stdout, Kafka, Postgres, GCS, MinIO, BigTable, or your own plugin)
                │
                ▼
        Druid + UI API → Osprey UI
```

For the worker's rule-evaluation architecture (how a single event is scored against your rules), see the diagram at the top of [Writing Rules](../rules/). For the coordinator's internals (priority queues, bidirectional streaming vs. synchronous API), see the [Coordinator README](https://github.com/roostorg/osprey/tree/main/example_docker_compose/run_osprey_with_coordinator), which has a more detailed component diagram and a working example.

## Getting data in

There are three ways to get an event into Osprey:

### 1. Kafka (the default)

By default, the worker consumes from the Kafka topic `osprey.actions_input` (`OSPREY_KAFKA_INPUT_STREAM_TOPIC` in `docker-compose.yaml`; see `KafkaInputStream` in `osprey_worker/src/osprey/worker/sinks/sink/input_stream.py`). Each message is a JSON blob shaped like:

```json
{
  "send_time": "<Go-formatted timestamp>",
  "data": {
    "action_id": 123,
    "action_name": "user_login",
    "data": { "...": "your event's actual fields" }
  }
}
```

`action_name` is what your rules match against; the inner `data` object is whatever fields your event has, and those become the values your UDFs and feature extractors read.

### 2. Google Cloud PubSub

An alternate input source, selected via `InputStreamSource.PUBSUB` (`osprey_worker/src/osprey/worker/sinks/input_stream_chooser.py`). Configured with `PUBSUB_OSPREY_PROJECT_ID` and `PUBSUB_OSPREY_RULES_SINK_SUBSCRIPTION`.

### 3. The Coordinator's synchronous gRPC API

If you run Osprey with the optional Rust coordinator (`osprey_coordinator/`), external services can submit a single action directly and get an immediate response, without going through Kafka at all, which is useful when a caller needs a synchronous verdict rather than firing into a queue. This is the Sync Action API on port `19951` (bidirectional streaming for workers is a separate port, `19950`). See the [Coordinator README](https://github.com/roostorg/osprey/tree/main/example_docker_compose/run_osprey_with_coordinator) for a working `grpcurl` example and full setup.

### Running against your own platform, without forking

If your events don't come from Kafka/PubSub/the coordinator, or don't arrive as that JSON envelope, you don't need to fork Osprey. Two plugin hooks handle exactly this:

- **`register_action_proto_deserializer`**: convert your own protobuf `Action` message into the JSON dict shape the engine expects.
- **`register_input_stream`**: swap in an entirely custom input source (a different queue system, a database poller, whatever you have).

Both are `pluggy` hooks, registered the same way as UDFs and output sinks. See [Integrations & Plugins](integrations.md) for the plugin mechanics and `example_plugins/src/register_plugins.py` for a working example.

## Getting data out

Once a rule evaluates, the result flows out through two, largely independent mechanisms:

### Output sinks

An `ExecutionResult` is handed to every registered `BaseOutputSink` (`register_output_sinks`). Stdlib ships:
- `StdoutOutputSink`: prints to stdout (the default if nothing else is configured; useful for local dev)
- `KafkaOutputSink`: writes to `osprey.execution_results` by default (`OSPREY_KAFKA_OUTPUT_TOPIC`)
- `StoredExecutionResultOutputSink`: persists via whichever `ExecutionResultStore` is configured (see below)

You can register your own via the same hook; it's the extension point for pushing results into a review queue, a webhook, or any other external system.

### Execution result storage

Separately, `ExecutionResultStore` backends persist full execution results for later querying. The active backend is chosen via `OSPREY_EXECUTION_RESULT_STORAGE_BACKEND` (`osprey_worker/src/osprey/worker/_stdlibplugin/execution_result_store_chooser.py`):

| Value | Backend |
| --- | --- |
| `bigtable` | Google Cloud BigTable |
| `gcs` | Google Cloud Storage |
| `minio` | MinIO (S3-compatible; `OSPREY_MINIO_ENDPOINT`, `OSPREY_MINIO_ACCESS_KEY`, `OSPREY_MINIO_SECRET_KEY`, `OSPREY_MINIO_EXECUTION_RESULTS_BUCKET`) |
| `postgres` | Postgres |
| `plugin` | Your own `register_execution_result_store` implementation |
| `none` (default) | No persistence |

### How results become queryable in the UI

Druid consumes the worker's Kafka output and powers the UI API's real-time querying; this is what backs the [Investigate](../user/investigate/) query interface and the [Rules](../user/manage.md#rules-registry)/[Features Registries](../user/manage.md#features-registry). If you're running without Kafka output enabled, Druid has nothing to index and the query UI will be empty even though rules are still evaluating correctly.
