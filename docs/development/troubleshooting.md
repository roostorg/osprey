# Troubleshooting

Solve common issues.

## Druid schema not updating

If Druid is not ingesting data or is stuck on a stale schema after changing rule output structure, reset the Kafka supervisor and resubmit the ingestion spec:

```bash
curl -X POST http://localhost:8888/druid/indexer/v1/supervisor/osprey.execution_results/terminate
docker compose restart druid-spec-submitter
```

To wipe all Druid and MinIO state and start fresh (Postgres data is preserved):

```bash
docker compose down
docker volume rm osprey_middle_var osprey_historical_var osprey_broker_var osprey_coordinator_var osprey_router_var osprey_druid_shared osprey_minio_data
docker compose up -d
```

To wipe everything including Postgres:

```bash
docker compose down -v && docker compose up -d
```

## Test data not appearing in the UI

The UI defaults to querying the last 24 hours. If the selected time range is too large (weeks or months), Druid scans across many segments and results can be slow or appear empty.

- Narrow the time range to 1–4 hours centered on when you generated test data
- Click the edit icon next to the displayed time range to switch to a custom date/time picker
- Note that Druid's Kafka consumer uses `auto.offset.reset: latest`; it only picks up events produced after `docker compose up` first ran, so events from before that point will not appear regardless of the time range

## Kafka topic disk growth

Topics are created with a 48-hour/8 GB-per-partition retention limit. If you deployed before this was set, apply the config to your existing topics:

```bash
for topic in osprey.actions_input osprey.execution_results; do
  kafka-configs --bootstrap-server localhost:9092 \
    --entity-type topics --entity-name $topic --alter \
    --add-config retention.ms=172800000,retention.bytes=8589934592,segment.bytes=1073741824
done
```

## "uv: command not found"

Install uv using the installation script or pip:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# Then restart your terminal
```

## Pre-commit hooks failing

Run hooks manually to see detailed errors:

```bash
uv run pre-commit run --all-files
```

## MyPy errors on protobuf files

**Solution**: Protobuf generated files are excluded in configuration. If you see errors, check that files match the exclusion patterns in `pyproject.toml`.

## Import errors during type checking

Ensure all dependencies are installed:

```bash
uv sync
```
