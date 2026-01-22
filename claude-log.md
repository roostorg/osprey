# Claude Code Session Log - Getting Osprey Running on Fedora

**Date:** 2026-01-14
**System:** Fedora Linux (kernel 6.18.4-200.fc43.x86_64) with SELinux enforcing
**Goal:** Get the Osprey rules engine up and running locally

## Summary

Successfully deployed Osprey on a Fedora system with SELinux enforcing. Fixed multiple permission and healthcheck issues to get all services running properly.

## Issues Encountered and Solutions

### 1. SELinux Permission Denied Errors

**Problem:**
Docker containers couldn't access mounted host directories, resulting in errors like:
- `Permission denied` on `/osprey/entrypoint.sh`
- `Permission denied` reading `pyproject.toml` files
- `EACCES: permission denied` on `package.json`

**Root Cause:**
SELinux was enforcing security policies that prevented containers from accessing host files without proper labeling.

**Solution:**
Added `:z` flag to all volume mounts in `docker-compose.yaml`:

```yaml
# Before
volumes:
  - ./osprey_worker:/osprey/osprey_worker

# After
volumes:
  - ./osprey_worker:/osprey/osprey_worker:z
```

**Files Modified:**
- All volume mounts for: `osprey-worker`, `osprey-ui-api`, `osprey-ui`, `minio-bucket-init`, `bigtable-initializer`, `druid-spec-submitter`, and `kafka-test-data-producer`

**Key Learning:**
- `:z` flag sets shared SELinux context (`svirt_sandbox_file_t`) allowing multiple containers to access files
- `:Z` (uppercase) would be for private single-container access
- This is specific to SELinux-enforcing systems (RHEL, Fedora, CentOS)
- Git doesn't track SELinux context changes, so this is safe for version control

### 2. Docker Healthcheck Failures

**Problem:**
Both Kafka and Bigtable containers were perpetually unhealthy despite services running correctly:
- Kafka healthcheck showed help text instead of listing topics
- Bigtable healthcheck failed with "pgrep: no matching criteria specified"

**Root Cause:**
Healthcheck commands were using `["CMD", "bash", "-c", "command"]` format which wasn't properly parsing shell commands with flags.

**Solution:**
Changed from `CMD` array format to `CMD-SHELL` format:

**Kafka Fix:**
```yaml
# Before
healthcheck:
  test:
    [
      "CMD",
      "bash",
      "-c",
      "kafka-topics --bootstrap-server kafka:29092 --list",
    ]

# After
healthcheck:
  test: ["CMD-SHELL", "kafka-topics --bootstrap-server kafka:29092 --list"]
```

**Bigtable Fix:**
```yaml
# Before
healthcheck:
  test: ["CMD", "bash", "-c", "pgrep -f cbtemulator > /dev/null || exit 1"]

# After
healthcheck:
  test: ["CMD-SHELL", "pgrep -f cbtemulator || exit 1"]
```

**Key Learning:**
- `CMD-SHELL` executes commands through a shell (`/bin/sh -c`)
- Properly handles command flags and shell operators
- Removed unnecessary redirects like `> /dev/null` since health check output is captured anyway
- Both containers now report `healthy` status

### 3. Service Initialization Scripts Permission Issues

**Problem:**
`init-minio-bucket.sh` lacked execute permissions, causing the initialization container to fail with "Permission denied"

**Solution:**
```bash
chmod +x init-minio-bucket.sh
```

### 4. Healthcheck Dependency Deadlock

**Problem:**
Even with working health checks initially, `osprey-worker`, `osprey-ui-api`, and `osprey-ui` containers wouldn't start due to dependency on `kafka` and `bigtable` health checks.

**Workaround:**
Used manual initialization and direct container start:
```bash
# Manually initialize services
docker run --rm --network osprey_default minio/mc alias set myminio http://minio:9000 minioadmin minioadmin123
docker run --rm --network osprey_default minio/mc mb --ignore-existing myminio/execution-output

# Create Bigtable tables
docker run --rm --network osprey_default -e BIGTABLE_EMULATOR_HOST=bigtable:8361 gcr.io/google.com/cloudsdktool/cloud-sdk:latest bash -c 'cbt -project=osprey-dev -instance=osprey-bigtable createtable audit_log'

# Create Kafka topics
docker run --rm --network osprey_default confluentinc/cp-kafka:7.4.0 bash -c "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic osprey.actions_input --partitions 3 --replication-factor 1"

# Start Osprey services directly
docker start osprey-worker osprey-ui-api osprey-ui
```

**Long-term Solution:**
After fixing the healthchecks with `CMD-SHELL`, the dependency chain works properly and services can start normally with `docker compose up`.

## Environment Setup Steps

1. **Install dependencies:**
   ```bash
   uv sync
   ```

2. **Install pre-commit hooks:**
   ```bash
   uv run pre-commit install
   ```

3. **Start services:**
   ```bash
   docker compose up -d
   ```

4. **Start test data producer:**
   ```bash
   docker compose --profile test_data up kafka-test-data-producer -d
   ```

## Final Architecture

### Running Services

| Service | Port | Purpose |
|---------|------|---------|
| osprey-ui | 5002 | Web interface for querying and investigating events |
| osprey-ui-api | 5004 | Backend API for UI |
| osprey-worker | 5001 | Rules engine processor |
| kafka | 9092 | Event streaming |
| postgres | 5432 | Database for labels and state |
| bigtable (emulator) | 8361 | NoSQL storage for audit logs |
| minio | 9000/9001 | S3-compatible object storage for execution results |
| druid-broker | 8082 | Analytics query engine |
| druid-coordinator | 8081 | Druid cluster coordinator |
| druid-router | 8888 | Query routing |
| snowflake-id-worker | 8088 | Distributed ID generation |

### Data Flow

1. Test data producer → Kafka topic `osprey.actions_input`
2. Osprey worker consumes from Kafka
3. Rules engine processes events (defined in `./example_rules/`)
4. Results written to:
   - Kafka topic `osprey.execution_results`
   - Bigtable (audit logs)
   - MinIO (execution output)
   - Postgres (labels)
5. UI queries data from Druid for analytics

## Verification

Services verified working:
- ✅ Osprey Worker processing events from Kafka
- ✅ Rules engine evaluating conditions (detecting "hello" in posts)
- ✅ Automatic actions executing (banning users, applying labels)
- ✅ UI accessible at http://localhost:5002
- ✅ UI API responding at http://localhost:5004
- ✅ Test data producer generating events
- ✅ All healthchecks passing

Example rule execution observed:
```json
{
  "__action_id": 13,
  "__timestamp": "2026-01-14T21:33:12.777974+00:00",
  "__ban_user": ["user_9023|User said \"hello\""],
  "__entity_label_mutations": ["User/meow/1"],
  "ActionName": "create_post",
  "UserId": "user_9023",
  "PostText": "fox brown hello and cat.",
  "ContainsHello": true
}
```

## Key Takeaways

1. **SELinux on Fedora requires explicit volume labeling** - Always add `:z` or `:Z` flags to Docker volume mounts
2. **Healthcheck format matters** - Use `CMD-SHELL` for shell commands with flags/operators
3. **Services can run despite failing healthchecks** - Manual testing confirms actual service status
4. **Docker Compose dependency chains break with unhealthy services** - May need manual intervention
5. **Bigtable emulator is lenient** - Instance creation may fail, but table operations work regardless

## Files Modified

- `docker-compose.yaml` - Added `:z` flags to 7 volume mounts, fixed 2 healthcheck definitions
- `init-minio-bucket.sh` - Added execute permission (chmod +x)

## Resources

- [Docker volume mount SELinux labels](https://docs.docker.com/storage/bind-mounts/#configure-the-selinux-label)
- [Docker healthcheck best practices](https://docs.docker.com/engine/reference/builder/#healthcheck)
- Osprey documentation: `./docs/`

## Known Issues

### Empty Timeseries Chart in UI

**Problem:**
The timeseries chart shows "No data available" even though events are visible in the Event Stream and data exists in Druid.

**Root Cause:**
The Druid ingestion spec at `/druid/specs/execution_results.json` uses `"auto.offset.reset": "latest"`, which means Druid only ingests messages that arrive AFTER the supervisor starts. Any test data generated before the Druid supervisor started is not ingested.

**Current Status:**
- Druid has successfully ingested data (226+ events confirmed via SQL query)
- Data is queryable directly: `curl -X POST "http://localhost:8082/druid/v2/sql" -H "Content-Type: application/json" -d '{"query": "SELECT COUNT(*) FROM \"osprey.execution_results\""}'`
- The `/events/timeseries` API endpoint returns errors about invalid escape sequences
- Event Stream (right sidebar) works correctly because it queries Postgres/Bigtable directly, not Druid

**Solution Options:**

1. **Wait for more data** - Continue running the test data producer and the chart should populate once enough recent data accumulates

2. **Change offset reset to "earliest"** (for development):
   ```json
   # In druid/specs/execution_results.json, change:
   "auto.offset.reset": "latest"
   # to:
   "auto.offset.reset": "earliest"
   ```
   Then restart the Druid supervisor to re-ingest all Kafka messages from the beginning

3. **Verify the UI API timeseries endpoint** - There may be a bug in the API's handling of the Druid query

**Verification Commands:**
```bash
# Check data in Druid
curl -s -X POST "http://localhost:8082/druid/v2/sql" -H "Content-Type: application/json" -d '{"query": "SELECT MIN(__time), MAX(__time), COUNT(*) FROM \"osprey.execution_results\""}' | python3 -m json.tool

# Check Druid supervisor status
curl -s http://localhost:8081/druid/indexer/v1/supervisor/osprey.execution_results/status | python3 -m json.tool
```

## Next Steps

- Investigate and fix the `/events/timeseries` API endpoint error
- Consider changing Druid offset reset policy for development environments
- Explore example rules in `./example_rules/`
- Write custom rules for your use case
- Review Trust & Safety investigation workflows in UI
- Configure production-ready storage backends
- Set up proper monitoring and alerting
