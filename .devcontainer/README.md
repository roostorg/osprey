# Osprey TrustCon demo Codespace

One-click GitHub Codespace that boots the full Osprey real-time rules engine so
you can watch it process content and fire rules in the dashboard. No local setup.

## How to run

1. Open this repo in a Codespace on the **4-core / 16 GB** machine (see the RAM
   caveat below). Use the machine-type picker when creating the Codespace.
2. Wait for the setup to finish. `postCreateCommand` runs
   `.devcontainer/setup.sh`, which pulls images and brings up the whole stack.
   First run takes several minutes (Kafka, Postgres, and the Druid images are large).
3. In the **PORTS** tab, click the globe icon next to **port 5002 (Osprey UI)**.
   That opens the dashboard.

## What boots

The stack comes up via `docker compose`:

- Kafka + Zookeeper (event bus)
- Postgres (state)
- a full Druid cluster: coordinator, broker, historical, middlemanager, router (analytics store the UI reads)
- MinIO (object storage)
- **osprey-worker** (the rules engine)
- **osprey-ui-api** (Flask HTTP API)
- **osprey-ui** (React dashboard)
- an event source (see below)

## Event source: synthetic vs live Bluesky

`setup.sh` picks the source automatically:

- **Live Bluesky JetStream** if `run-atproto.sh` and `docker-compose.atproto.yaml`
  are present on the branch (the maintainer's atproto/jetstream work). Osprey
  reads the real Bluesky firehose and runs the `example_atproto_rules` ruleset
  (e.g. `PostContainsTestRule` labels an author `test-poster` on posts containing
  "test").
- **Synthetic producer** otherwise: `demo.sh` starts the `test_data` compose
  profile, a ~1-event/second generator, and runs `example_rules` (ContainsHello,
  LazyPostRule, QuickPostRule, FoxPostRule).

## Forwarded ports

| Port | Service |
| ---- | ------- |
| 5002 | Osprey UI (the dashboard) |
| 5004 | Osprey UI API |
| 5001 | Osprey worker |
| 8888 | Druid console |
| 9001 | MinIO console |

## RAM caveat

The integration stack documents **~8 GB RAM just for the app**, and this brings up
Kafka + a full Druid cluster + MinIO + the worker + UI + UI API simultaneously. A
2-core / 8 GB Codespace will thrash or OOM. `devcontainer.json` requests 4 cores /
16 GB / 64 GB storage for this reason. If services fail to start, the machine is
almost certainly too small.

## Editing a rule

Rules are plain text under the active rules directory (`example_rules/` or
`example_atproto_rules/`), mounted into the worker. Edit a phrase, then reload:

```bash
docker compose restart osprey-worker
```

New events will match your edited rule in the Event Stream.

## Handy commands

```bash
docker compose ps                        # what's running
docker compose logs -f osprey-worker     # follow the engine
docker compose down                      # stop (keep data)
docker compose --profile test_data down -v   # stop and wipe (synthetic path)
```
