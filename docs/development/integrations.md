# Integrations & Plugins

Osprey is extended through a [pluggy](https://pluggy.readthedocs.io/)-based plugin system. A plugin package implements any subset of the hooks in the [Available hooks](README.md#available-hooks) table and registers them via `@hookimpl_osprey`; see `example_plugins/src/register_plugins.py` and `example_plugins/src/register_async_plugins.py` for working examples. This page walks through the concrete integration points adopters ask about most.

## User-defined functions (UDFs)

UDFs are how you extend the functions available to rule authors — see [Writing Rules § User Defined Functions](../rules.md#user-defined-functions-udfs) for the language-level reference, including working examples (`TextContains`, `BanUser`) in `example_plugins/src/udfs/`.

To register your own, implement a `UDFBase` subclass and return it from the `register_udfs` hook. Give it a category from `UdfCategories` (`osprey_worker/src/osprey/engine/stdlib/udfs/categories.py` — e.g. `STRING`, `HASH`, `ENTITY`, `HTTP`) so it's grouped sensibly in the [UDF Registry](../user/manage.md#udf-registry).

## Input and output sinks

See [Data Flow § Getting data in](../data-flow.md#getting-data-in) and [§ Getting data out](../data-flow.md#getting-data-out) for the conceptual picture. The relevant hooks:

- `register_input_stream` / `register_action_proto_deserializer` — bring in events from a source other than Kafka/PubSub/the coordinator.
- `register_output_sinks` — send execution results somewhere other than stdout/Kafka/the configured result store.
- `register_execution_result_store` — persist results in a backend other than the built-in BigTable/GCS/MinIO/Postgres options.

## Hash-based lookups

There's no dedicated lookup-table or lookup-service feature in Osprey today — no denylist/allowlist primitive, no bulk-import-a-set-of-values mechanism. What exists is the `Hash*` UDF family (`osprey_worker/src/osprey/engine/stdlib/udfs/string_hashes.py`: `HashMd5`, `HashSha1`, `HashSha256`, `HashSha512`), which you compose with SML's `in` operator or `HasLabel` to check membership:

```python
# Check a hashed value against a small inline set
IsKnownBadHash = HashSha256(input=SomeValue) in ['abc123...', 'def456...']

# Or check membership via a label that was set by some other process
IsKnownBadActor = HasLabel(entity=SomeEntity, label='KnownBad')
```

If you need to check against a large external list (millions of hashes, updated frequently), that's not a built-in feature — write a custom UDF (see above) that queries your own store.

## Plugging in your own ML models

There's no dedicated hook for this either — it's the standard UDF pattern. Implement a `UDFBase` subclass whose `execute()` calls out (HTTP, gRPC, an SDK — whatever your model server expects) and returns a score, boolean, or other typed value, then register it via `register_udfs`. There's no built-in outbound-HTTP UDF to build on (`UdfCategories.HTTP` currently has one UDF, `extract_cookie.py`, which parses cookies rather than making requests).

Because model calls are often slow or costly, gate them with `Require(..., require_if=...)` so they only run when relevant — `docs/rules.md` documents this pattern directly for "a call to an AI service":

```python
Require(rule='ai_services/my_ai_service.sml', require_if=ActionName == 'register')
```

## Connecting to a review tool

This isn't a shipped integration — "poor integration with review tools (e.g., build filtered review queues)" is listed as an unmet need in [User Research & Personas](../research-personas.md), not something Osprey currently connects to out of the box. The closest real extension points are the output-sink hooks: `register_output_sinks` for execution results generally, or `register_label_output_sink` for a custom sink specifically for label mutations (replacing the default `LabelOutputSink`). Either could push into a review queue, but there's no purpose-built connector today.

## Labels service

If you want Osprey to track state across events (e.g., "this user has 3 prior violations"), implement `register_labels_service_or_provider`. `example_plugins/src/services/labels_service.py` is a full reference implementation backed by Postgres — a good starting point if you're building your own.
