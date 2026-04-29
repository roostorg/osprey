# ClickHouse Local Dev Footprint

This note compares the default Druid-based local dev stack with the ClickHouse
overlay for the same Osprey UI event-query workflow.

It exists to answer a practical question: what do we save locally by using the
ClickHouse reference backend instead of the default Druid stack?

## Snapshot

Measured on an arm64 local Docker Desktop setup on April 9, 2026.

On Apple Silicon, the upstream Druid images used by the default stack run under
`linux/amd64` emulation in Docker Desktop. That affects the cold-start and CPU
numbers below. The `etcd` image used by the Docker test stacks is also
`amd64`-only today.

Included in the comparison:

- running long-lived dev containers only
- `docker stats --no-stream` memory and CPU snapshots

Excluded from the comparison:

- one-shot init containers such as `minio-bucket-init`
- topic/bootstrap helpers such as `osprey-kafka-topic-creator`
- optional profile services such as `bigtable`
- seed-data producers

| Mode | Running containers | Memory | CPU snapshot |
| --- | ---: | ---: | ---: |
| Druid dev (`make up`) | 13 | about 6.05 GiB | about 435.5% |
| ClickHouse dev (`make clickhouse-up`) | 8 | about 3.34 GiB | about 250.9% |

Observed delta:

- 5 fewer long-lived containers
- about 2.71 GiB less memory
- about 42% lower CPU in the sampled snapshot

## Cold Timing

The same machine was also measured from a clean repo-scoped Docker state, with
image fetch/build time separated from runtime startup.

Query-ready means both of these were true:

- the frontend was reachable
- `/events/scan` returned `200` from the UI API

| Mode | `make *-fetch` | `make *-up` return time | Query-ready time | Total from clean start |
| --- | ---: | ---: | ---: | ---: |
| Druid dev (`make up`) | 19s | 58s | 168s | 203s |
| ClickHouse dev (`make clickhouse-up`) | 39s | 30s | 74s | 114s |

Observed delta:

- ClickHouse is about 89s faster to reach a queryable stack from a clean start
- Druid is faster to cold-fetch by about 20s
- the ClickHouse cold-fetch penalty is mostly the `clickhouse/clickhouse-server`
  image pull
- the Druid runtime penalty is the cluster taking longer to become actually
  queryable after `make up` returns

## Integration Test Timing

The isolated backend integration suites were re-timed on the same Apple
Silicon host.

| Command | Scope | Current observation |
| --- | --- | --- |
| `make druid-test PYTEST_ARGS='osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_druid_integration.py'` | full isolated Druid integration file | one successful cold run completed in about `178s`; a later cold rerun on the same host flaked and failed after `303s` |
| `make clickhouse-test PYTEST_ARGS='osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_clickhouse_integration.py'` | full isolated ClickHouse integration file | cold run passed in `49.20s` end-to-end (`13 passed` in `5.19s` of pytest time) |

Observed delta:

- ClickHouse completed the successful cold run about 129s faster than Druid
- the dominant cost on the Druid side is still full cluster startup rather than
  pytest body time
- Druid remains materially less stable on this Apple Silicon setup, so the
  cold-run cost is not only higher but less predictable
- this is why `make test` now stays backend-neutral and the Druid and
  ClickHouse integration suites run behind separate Make targets
- both suites now include ACL-composition cases, including `/events/topn/csv`,
  not just backend-only happy paths

## Why The Footprint Drops

Both modes still run the common local services:

- Kafka
- MinIO
- Postgres
- Snowflake ID worker
- Osprey worker
- Osprey UI API
- Osprey UI

The difference is the event-query backend layer.

Default Druid mode adds these long-lived services:

- `druid-zookeeper`
- `druid-coordinator`
- `druid-broker`
- `druid-historical`
- `druid-middlemanager`
- `druid-router`

ClickHouse mode replaces that cluster with a single:

- `clickhouse`

So the local savings are mostly the absence of the Druid cluster, not a change
in the Osprey application containers themselves.

## Largest Memory Consumers In The Sample

Druid mode:

- `osprey-ui`: about 1.82 GiB
- `osprey-druid-middlemanager`: about 1.25 GiB
- `osprey-druid-historical`: about 791 MiB
- `osprey-druid-broker`: about 586 MiB

ClickHouse mode:

- `osprey-clickhouse-ui`: about 1.75 GiB
- `osprey-clickhouse-clickhouse`: about 594 MiB
- `osprey-clickhouse-kafka`: about 438 MiB

That matters because the frontend dev container remains expensive in both
modes. The ClickHouse improvement comes from simplifying the event-query data
layer, not from making every container light.

## How To Reproduce

Start from a clean repo-scoped Docker environment:

```bash
make docker-clean
```

Measure image fetch/build time separately from startup time:

```bash
time make fetch
time make clickhouse-fetch
```

Then bring up each stack in isolation:

```bash
make down
make up
```

```bash
make clickhouse-down
make clickhouse-up
```

If you want the same readiness bar used for the numbers above, wait until the
UI API can answer an event query instead of only trusting `docker compose up`:

```bash
docker exec osprey-ui-api /osprey/.venv/bin/python -c "import json, urllib.request; payload={'start':'2026-04-08T00:00:00+00:00','end':'2026-04-09T23:59:59+00:00','query_filter':'','entity':None,'limit':1}; req=urllib.request.Request('http://127.0.0.1:5004/events/scan', data=json.dumps(payload).encode(), headers={'Content-Type':'application/json'}); resp=urllib.request.urlopen(req); print(resp.status)"
docker exec osprey-clickhouse-ui-api /osprey/.venv/bin/python -c "import json, urllib.request; payload={'start':'2026-04-08T00:00:00+00:00','end':'2026-04-09T23:59:59+00:00','query_filter':'','entity':None,'limit':1}; req=urllib.request.Request('http://127.0.0.1:5004/events/scan', data=json.dumps(payload).encode(), headers={'Content-Type':'application/json'}); resp=urllib.request.urlopen(req); print(resp.status)"
```

Capture a steady-state snapshot after startup settles:

```bash
docker stats --no-stream
```

For container counts, use the compose project rather than `docker ps` alone so
stale optional containers from earlier profile runs do not skew the numbers:

```bash
docker compose -p osprey -f docker-compose.yaml ps
docker compose -p osprey-clickhouse -f docker-compose.yaml -f docker-compose.clickhouse.yaml --profile clickhouse ps
```

## Caveats

- CPU percentages from `docker stats` are bursty and depend on when the sample
  is taken.
- Memory and container counts are more useful than one CPU snapshot.
- Absolute values will vary by host architecture, Docker Desktop configuration,
  and whether images and dependencies were already warm.
- On Apple Silicon, the Druid numbers include the cost of `amd64` emulation.
