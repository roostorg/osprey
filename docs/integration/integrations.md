# Integrations & Plugins

Osprey is designed to be extended without modifying the core codebase; you can wire up your own logic such as detection functions, output destinations, entity state storage, and ML models through plugin packages that Osprey discovers at startup. A plugin package implements any subset of the hooks in the [Available hooks](../development/local.md#available-hooks) table; this page walks through the integration points adopters ask about most.

See the [`example_plugins/` directory](https://github.com/roostorg/osprey/tree/main/example_plugins) for a working reference package.

## How plugins are loaded

Osprey uses [pluggy](https://pluggy.readthedocs.io/) for plugin discovery. Your plugin package declares one or both of these entry-point groups in its `pyproject.toml`:

- `osprey_plugin`: loaded by the standard gevent worker
- `osprey_async_plugin`: loaded by the experimental asyncio worker

For example:

```toml
[project.entry-points.osprey_plugin]
register_plugins = "register_plugins"

[project.entry-points.osprey_async_plugin]
register_async_plugins = "register_async_plugins"
```

Each entry point resolves to a module that contains hook functions decorated with `@hookimpl_osprey` or `@hookimpl_osprey_async`. Osprey calls each hook at startup to collect your registrations; see [`example_plugins/src/register_plugins.py`](https://github.com/roostorg/osprey/blob/main/example_plugins/src/register_plugins.py) and [`register_async_plugins.py`](https://github.com/roostorg/osprey/blob/main/example_plugins/src/register_async_plugins.py) for more.

## Writing UDFs

A user-defined function (UDF) is a Python class that can be called from your rules. UDFs encapsulate reusable detection logic—text matching, DNS lookups, hash comparisons, ML inference—and expose it as a named function in the rules language. See [Writing Rules § User Defined Functions](../rules/README.md#user-defined-functions-udfs) for the language-level view.

### Anatomy of a UDF

UDFs require:

1. An **arguments** class (subclass of `ArgumentsBase`) that declares the parameters the UDF accepts with types
2. A **UDF class** (subclass of `UDFBase[Arguments, ReturnType]`) with an `execute` method that contains the logic

For example:

```python
# example_plugins/src/udfs/text_contains.py
import re

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase


class TextContainsArguments(ArgumentsBase):
    text: str
    phrase: str
    case_sensitive = False


class TextContains(UDFBase[TextContainsArguments, bool]):
    def execute(self, execution_context: ExecutionContext, arguments: TextContainsArguments) -> bool:
        escaped = re.escape(arguments.phrase)

        pattern = rf'\b{escaped}\b'

        flags = 0 if arguments.case_sensitive else re.IGNORECASE
        regex = re.compile(pattern, flags)

        return bool(regex.search(arguments.text))
```

Once registered, `TextContains` is callable from rules as:

```python
TextContains(text=SomeFeature, phrase="spam")
```

### UDFs with side effects

UDFs can also produce **effects**: structured outputs that downstream systems act on, such as banning a user or flagging content. Effects are expressed using `EffectBase` as the return type. See [`example_plugins/src/udfs/ban_user.py`](https://github.com/roostorg/osprey/blob/main/example_plugins/src/udfs/ban_user.py) for an example.

### Async UDFs

UDFs that perform I/O (e.g. network calls or database reads) should subclass `AsyncUDFBase` when used in the async worker; see [`osprey_async_worker/src/osprey/async_worker/stdlib_udfs/async_mx_lookup.py`](https://github.com/roostorg/osprey/blob/main/osprey_async_worker/src/osprey/async_worker/stdlib_udfs/async_mx_lookup.py) for an example. Pure-computation UDFs like `TextContains` can be reused in both workers without modification.

### Registering UDFs

Return your UDF classes from the `register_udfs` hook; for example:

```python
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey

@hookimpl_osprey
def register_udfs():
    return [TextContains, BanUser]
```

Give each UDF a category from `UdfCategories` (`osprey_worker/src/osprey/engine/stdlib/udfs/categories.py`, e.g. `STRING`, `HASH`, `ENTITY`, `HTTP`) so it's grouped sensibly in the [UDF Registry](../user/manage.md#udf-registry).

## Hash-based lookups

Osprey's standard library includes the `Hash*` UDF family (`HashMd5`, `HashSha1`, `HashSha256`, `HashSha512`, under the `HASH` category and available without registration), which takes a string `input` and returns the hex digest. Compose these with SML's `in` operator or `HasLabel` to check values against known-bad sets without storing raw data:

```python
# Check a hashed value against a small inline set
IsKnownBadHash = HashSha256(input=SomeValue) in ['abc123...', 'def456...']

# Or check membership via a label that was set by some other process
IsKnownBadActor = HasLabel(entity=SomeEntity, label='KnownBad')
```

Inline sets suit small, stable lists. There's no bulk-import or lookup-table primitive, so for a large external list (millions of hashes, updated frequently), write a custom UDF (see above) that queries your own store.

## Configuring input sinks

An input sink is where events _enter_ Osprey. Osprey ships with built-in sources (Kafka, Google Pub/Sub, the Osprey Coordinator, and a synthetic generator for local testing) selected via the `InputStreamSource` config value. If none of those fit your platform, you can register a custom input stream as a plugin. For the conceptual picture, see [Data Flow § Getting data in](data-flow.md#getting-data-in).

### Built-in sources

The worker picks an input stream based on `InputStreamSource`:

Source               | Config                                                              | Use case
-------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------
`KAFKA`              | `OSPREY_KAFKA_INPUT_STREAM_TOPIC`, `OSPREY_KAFKA_BOOTSTRAP_SERVERS` | Consume Action events from a Kafka topic
`PUBSUB`             | `PUBSUB_OSPREY_PROJECT_ID`, `PUBSUB_OSPREY_RULES_SINK_SUBSCRIPTION` | Consume from Google Pub/Sub
`OSPREY_COORDINATOR` | `OSPREY_COORDINATOR_SERVICE_NAME`                                   | Pull work from the Osprey Coordinator service
`SYNTHETIC`          | &nbsp;                                                              | Generates random fake events; useful for local dev without any upstream system
`PLUGIN`             | &nbsp;                                                              | Delegates to your registered `register_input_stream` hook

Set `InputStreamSource.KAFKA` (or whichever fits your existing infrastructure) if you already have events flowing through Kafka or Pub/Sub. Otherwise, implement a custom input stream and set `InputStreamSource.PLUGIN` in your config. If your events arrive as protobuf rather than JSON, there's also a `register_action_proto_deserializer` hook for supplying your own deserializer.

### Writing a custom input stream

If your event source isn't Kafka or Pub/Sub (e.g. it's a webhook receiver, a different message queue, or a polling API), subclass `BaseInputStream` and implement `_gen`, a generator that yields one `Action` (wrapped in an `AckingContext`) per event. For example:

```python
from collections.abc import Iterator

from osprey.engine.executor.execution_context import Action
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext, NoopAckingContext


class MyInputStream(BaseInputStream[BaseAckingContext[Action]]):
    def __init__(self, my_client):
        super().__init__()
        self._client = my_client

    def _gen(self) -> Iterator[BaseAckingContext[Action]]:
        while True:
            raw_event = self._client.poll()  # block until the next event
            action = Action(
                action_id=int(raw_event['id']),
                action_name=raw_event['type'],
                data=raw_event['payload'],
                timestamp=raw_event['timestamp'],
            )
            yield NoopAckingContext(item=action)
```

`_gen` is called once and re-used. It should block and yield indefinitely rather than returning. Use `NoopAckingContext` unless your source needs explicit ack/nack (e.g. a queue with at-least-once delivery), in which case implement a custom `BaseAckingContext` that acks on success.

Register it from the hook, and set `InputStreamSource.PLUGIN` in your config so the worker picks it up; for example:

```python
@hookimpl_osprey
def register_input_stream(config):
    return MyInputStream(my_client=build_client(config))
```

## Configuring output sinks

An output sink receives every `ExecutionResult` after rule evaluation and decides what to do with it, e.g. log it, forward it to a queue, call a webhook, or write to a database. For the conceptual picture, see [Data Flow § Getting data out](data-flow.md#getting-data-out); if what you want is to persist results in a backend other than the built-in BigTable/GCS/MinIO/Postgres options, the `register_execution_result_store` hook covers that instead.

### Sync output sink

Subclass `BaseOutputSink` and implement its methods; for example:

```python
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from osprey.engine.executor.execution_context import ExecutionResult


class MyOutputSink(BaseOutputSink):
    def will_do_work(self, result: ExecutionResult) -> bool:
        # Return False to skip this result early (e.g. filter by rule hit)
        return True

    def push(self, result: ExecutionResult) -> None:
        # Do something with the result—send to a queue, call an API, etc.
        pass

    def stop(self) -> None:
        # Clean up connections, flush buffers
        pass
```

Register it from the hook; for example:

```python
@hookimpl_osprey
def register_output_sinks(config):
    return [MyOutputSink()]
```

### Async output sink

For the async worker, subclass `AsyncBaseOutputSink` and make `push` and `stop` coroutines. See [`example_plugins/src/async_sinks/example_async_output_sink.py`](https://github.com/roostorg/osprey/blob/main/example_plugins/src/async_sinks/example_async_output_sink.py); for example:

```python
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
import logging

logger = logging.getLogger(__name__)

class ExampleAsyncOutputSink(AsyncBaseOutputSink):
    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        logger.info(
            'example async output sink: features=%s verdicts=%s',
            result.extracted_features_json,
            result.verdicts,
        )

    async def stop(self) -> None:
        pass
```

Register it with `@hookimpl_osprey_async` under the hook name `register_async_output_sinks`. This is a different hook from the sync `register_output_sinks` above, and goes in your `register_async_plugins.py` module (the one wired to the `osprey_async_plugin` entry point); for example:

```python
from osprey.async_worker.adaptor.plugin_manager import hookimpl_osprey_async

@hookimpl_osprey_async
def register_async_output_sinks(config):
    return [ExampleAsyncOutputSink()]
```

## Labels service

Osprey tracks state across events through entity labels: arbitrary tags attached to users, accounts, or other entities (e.g., "this user has 3 prior violations"). Labels are read during rule evaluation and written by rules with label effects. To persist labels across process restarts (and share them between workers), you provide a `LabelsServiceBase` implementation backed by your own storage via the `register_labels_service_or_provider` hook.

The example implementation in [`example_plugins/src/services/labels_service.py`](https://github.com/roostorg/osprey/blob/main/example_plugins/src/services/labels_service.py) uses PostgreSQL, e.g.:

```python
from osprey.worker.lib.storage.labels import LabelsServiceBase

class PostgresLabelsService(LabelsServiceBase):
    def initialize(self) -> None:
        # Called once at startup—open connections here
        ...

    def read_labels(self, entity) -> EntityLabels:
        # Return labels for this entity from your store
        ...

    @contextmanager
    def read_modify_write_labels_atomically(self, entity):
        # Yield the current labels; caller mutates them in place;
        # persist the result before the context manager exits
        ...
```

Register it from the hook:

```python
@hookimpl_osprey
def register_labels_service_or_provider(config):
    return PostgresLabelsService()
```

## Connecting to a review tool

Osprey doesn't currently support direct integration with a review tool; however, these extension points can help you integrate:

- `register_output_sinks`—push execution results into a review queue as they're produced.
- `register_label_output_sink`—a sink specifically for label mutations, replacing the default `LabelOutputSink`.
- A labels service backed by your existing datastore (previous section)—label an entity "flagged" from a rule, and your review queue queries your own store for that label.

## Plugging in your own ML model

ML models can be integrated as UDFs. For an in-process model, wrap your model's `predict` call in `execute`. Since a UDF's `__init__` receives `validation_context` and `arguments` from the framework, override it to accept and forward both, then do your model loading after the `super().__init__()` call; for example:

```python
class Arguments(ArgumentsBase):
    text: str

class MySpamClassifier(UDFBase[Arguments, float]):
    def __init__(self, validation_context, arguments):
        super().__init__(validation_context, arguments)
        self._model = load_model("/path/to/model.pkl")

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> float:
        return self._model.predict_proba([arguments.text])[0][1]
```

The returned score is then available in rules, e.g.:

```python
MySpamClassifier(text=MessageContent) > 0.85
```

Osprey constructs one UDF instance per call site when the rules are compiled, not per event, so the model isn't reloaded for every event processed. That's one instance per _call site_, not per _class_: if you call the same UDF from multiple rules, each call site gets its own instance, and each one loads its own copy of the model. **For a large model, prefer calling the UDF from a single rule (or share the loaded weights via a module-level cache) rather than invoking it from many places.**

For a model served remotely, the same pattern applies with `execute()` calling out over HTTP, gRPC, or your model server's SDK (you bring the client code). Because remote model calls are often slow or costly, gate them so they only run when relevant, using [Writing Rules' `Require(..., require_if=...)` pattern](../rules/README.md#workflow-structure-and-file-placement):

```python
Require(rule='ai_services/my_ai_service.sml', require_if=ActionName == 'register')
```

## Packaging your plugin

Your plugin package needs a `pyproject.toml` that declares the entry points; for example:

```toml
[project]
name = "my-osprey-plugins"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["pluggy==1.5.0"]

[tool.setuptools]
package-dir = {"" = "src"}

[tool.setuptools.packages.find]
where = ["src"]

[project.entry-points.osprey_plugin]
register_plugins = "register_plugins"
```

Install it into the same environment as Osprey and it will be discovered automatically on the next startup.

See also: [Writing Rules](../rules/)
