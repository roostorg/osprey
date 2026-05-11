# Adding Event Query Backends

This guide covers how to add a new event-query backend for the UI API, using BigQuery as the example target.

For the local Docker footprint tradeoff between the default Druid dev stack and
the ClickHouse overlay, see [ClickHouse Local Footprint](clickhouse_local_footprint.md).

There are two supported implementation paths:

1. Add a new built-in backend in the main repo, like Druid or ClickHouse.
2. Add a plugin backend and select it with `OSPREY_EVENT_QUERY_BACKEND=plugin`.

For a first implementation, the plugin path is usually cleaner. It avoids touching the default backend selection logic and keeps provider-specific dependencies and config out of the core path until the provider is stable.

This abstraction only covers event-query execution for the UI API. Execution-result
storage is a separate concern, selected independently with
`OSPREY_EXECUTION_RESULT_STORAGE_BACKEND`. A backend like BigQuery may choose to
own both pieces, but it should treat them as separate contracts.

## Current Contract

The UI event routes already go through a backend abstraction. The main contract points are:

- Backend protocol: `osprey_worker/src/osprey/worker/ui_api/osprey/lib/event_query_backend.py`
- Plugin hook spec: `osprey_worker/src/osprey/worker/adaptor/hookspecs/osprey_hooks.py`
- Stdlib backend registration: `osprey_worker/src/osprey/worker/_stdlibplugin/event_query_backend_register.py`
- Shared request/result models: `osprey_worker/src/osprey/worker/ui_api/osprey/lib/event_queries.py`
- Event routes: `osprey_worker/src/osprey/worker/ui_api/osprey/views/events.py`

The request/result models still have `Druid` in their names for compatibility, but they are now the shared API models for all event-query backends. `druid.py` re-exports those models for backward compatibility. A new backend should reuse the shared models instead of introducing `BigQueryQuery`, `ClickHouseQuery`, and similar duplicates.

## Query Translation Layer

New backends consume the neutral filter IR, not the original AST directly.

Relevant files:

- IR definitions: `osprey_worker/src/osprey/engine/query_language/filter_ir.py`
- AST -> IR translator: `osprey_worker/src/osprey/engine/query_language/ast_filter_ir_translator.py`
- Druid renderer: `osprey_worker/src/osprey/engine/query_language/druid_filter_translator.py`
- Druid compatibility wrapper: `osprey_worker/src/osprey/engine/query_language/ast_druid_translator.py`
- Query UDF base: `osprey_worker/src/osprey/engine/udf/base.py`
- Built-in query UDF examples:
  - `osprey_worker/src/osprey/engine/query_language/udfs/did_declare_verdict.py`
  - `osprey_worker/src/osprey/engine/query_language/udfs/did_mutate_label.py`
  - `osprey_worker/src/osprey/engine/query_language/udfs/regex_match.py`

Rules:

- A new backend should render from `FilterExpression`.
- Do not add backend-specific AST parsing unless the neutral IR is genuinely missing an operation.
- If a query UDF is expected to work in the new backend, it must implement `to_filter_ir()`.
- `to_druid_query()` exists only as a compatibility fallback for Druid.

If BigQuery needs semantics that the current IR cannot express, extend `filter_ir.py`, update `ast_filter_ir_translator.py`, and add tests before writing the backend renderer.

## Files To Add Or Modify

### Built-in backend in the main repo

Add:

- `osprey_worker/src/osprey/worker/ui_api/osprey/lib/bigquery.py`
  - Implement `EventQueryBackend`
  - Reuse the shared request/result models from `event_queries.py`
  - Render neutral IR into provider-specific query language
- `osprey_worker/src/osprey/worker/ui_api/osprey/lib/bigquery_client_holder.py`
  - Follow the `ClickHouseClientHolder` pattern
  - Initialize lazily from `CONFIG`
  - Keep provider-specific auth and connection setup here
- `osprey_worker/src/osprey/worker/ui_api/osprey/lib/tests/test_bigquery.py`
  - Unit tests for filter rendering and query generation
- `osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_bigquery_integration.py`
  - Integration coverage for `/events/scan`, `/events/timeseries`, `/events/groupby/approximate-count`, and `/events/topn`

Modify:

- `osprey_worker/src/osprey/worker/ui_api/osprey/singletons.py`
  - Add `BIGQUERY: Singleton[BigQueryClientHolder]`
- `osprey_worker/src/osprey/worker/ui_api/osprey/lib/event_query_backend.py`
  - Add a `bigquery` branch in `get_event_query_backend()`
- `pyproject.toml`
  - Add any new dependency if needed
  - BigQuery client libraries are already present in this repo, so a BigQuery backend may not need a new package
- `uv.lock`
  - Regenerate if dependencies change
- `docs/DEVELOPMENT.md`
  - Document backend-specific env vars and local/integration test flow

Optional, only if the backend needs local infra:

- `docker-compose.<backend>.yaml`
  - Dev overlay for a local service or emulator
- `docker-compose.test.<backend>.yaml`
  - Isolated test overlay
- `Makefile`
  - Add `<backend>-up`, `<backend>-down`, `<backend>-test`, and cleanup targets

Do not add:

- New Flask routes for the same event-query operations
- New request/response models duplicating the `*DruidQuery` classes

### Plugin backend

Add in your plugin package:

- A backend implementation module, for example `my_plugin/bigquery_backend.py`
- A registration hook in the plugin entrypoint, following `example_plugins/src/register_plugins.py`

Implement:

```python
@hookimpl_osprey
def register_event_query_backend(config: Config) -> EventQueryBackend | None:
    return BigQueryEventQueryBackend(config)
```

Use with:

```bash
OSPREY_EVENT_QUERY_BACKEND=plugin
```

For the plugin path, you do not need to modify:

- `osprey_worker/src/osprey/worker/ui_api/osprey/lib/event_query_backend.py`
- `osprey_worker/src/osprey/worker/_stdlibplugin/event_query_backend_register.py`

unless you later decide to promote the backend into the built-in set.

## Backend Implementation Checklist

For each backend method in `EventQueryBackend`, implement the equivalent behavior:

- `timeseries(query)`
- `groupby_approximate_count(query, **kwargs)`
- `topn(query, **kwargs)`
- `scan(query, query_filter_abilities=...)`

The shared query models in `druid.py` already provide useful helpers:

- `BaseDruidQuery._get_combined_filter_ir(...)`
  - Combines user filter, entity filter, and ACL filters into a single IR tree
- `parse_query_filter_ir(...)`
  - Parses raw query text into neutral IR
- `EventQueryBackend.build_period_data(...)`
  - Shapes provider result rows into the existing period/result response format
- `EventQueryBackend.build_topn_pop_response(...)`
  - Computes period-over-period diffs on shared `PeriodData`

A new backend should reuse those helpers where possible rather than reimplementing ACL composition, pagination semantics, or response shaping from scratch.

## Config Shape

The top-level selector is:

```bash
OSPREY_EVENT_QUERY_BACKEND=druid|clickhouse|plugin
```

For a built-in BigQuery backend, add provider-specific config keys in the same style as ClickHouse. For example:

```bash
BIGQUERY_PROJECT=...
BIGQUERY_DATASET=...
BIGQUERY_EVENT_TABLE=...
BIGQUERY_QUERY_TIMEOUT_SECONDS=30
```

If the backend uses cloud auth, prefer standard provider auth mechanisms instead of inventing a repo-specific credential format.

## Tests To Add

At minimum:

- `osprey_worker/src/osprey/engine/query_language/tests/test_ast_filter_ir_translator.py`
  - Extend only if the backend requires new IR semantics
- `osprey_worker/src/osprey/engine/query_language/tests/test_ast_druid_translator.py`
  - Keep this passing to protect Druid compatibility
- `osprey_worker/src/osprey/worker/ui_api/osprey/lib/tests/test_bigquery.py`
  - Backend renderer/query builder tests
- `osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_bigquery_integration.py`
  - Real backend integration tests

If you add a backend-specific integration suite, gate it the same way the ClickHouse suite is gated now:

- backend-specific compose overlay
- backend-specific env flag like `RUN_BIGQUERY_INTEGRATION_TESTS=true`
- dedicated `make` target for setup and teardown

## Extra Consumers

The Flask event routes already use the backend abstraction, but one CLI command still assumes Druid directly:

- `osprey_worker/src/osprey/worker/ui_api/osprey/cli.py`

If backend parity for that export flow matters, generalize it after the main UI API endpoints are working. It is not on the critical path for the event-query abstraction itself.

## Recommended Order

1. Implement the backend renderer and client holder.
2. Reuse the existing shared query models in `druid.py`.
3. Add unit tests for provider-specific query generation.
4. Add an integration test path.
5. Only then decide whether the backend should stay plugin-only or become built-in.
