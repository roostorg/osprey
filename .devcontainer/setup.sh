#!/usr/bin/env bash
# One-shot Codespace bring-up for the Osprey TrustCon demo.
#
# Goal: a non-technical workshop participant opens a Codespace, waits a few
# minutes, and watches Osprey's real-time rules engine process live content and
# fire rules in the UI. No manual steps required beyond opening the forwarded
# UI port.
#
# This script drives the repo's OWN bring-up scripts (demo.sh / run-atproto.sh)
# rather than reinventing them, so it stays correct as those evolve. It picks the
# event source automatically:
#
#   * If the Bluesky JetStream files are present (run-atproto.sh +
#     docker-compose.atproto.yaml, from the maintainer's atproto/jetstream work),
#     it runs Osprey against the LIVE Bluesky firehose.
#   * Otherwise it falls back to demo.sh, which drives a synthetic 1-event/second
#     Kafka producer (the test_data compose profile).
#
# Both paths bring up the full stack via docker compose and leave it running.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

log()  { printf '\n\033[0;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[0;32m  ok:\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  warn:\033[0m %s\n' "$*"; }

# ---------------------------------------------------------------------------
# 0. Sanity: Docker must be up (provided by the docker-in-docker feature).
# ---------------------------------------------------------------------------
log "Checking Docker is available"
for i in $(seq 1 30); do
  if docker info >/dev/null 2>&1; then ok "Docker daemon is running"; break; fi
  [ "$i" -eq 30 ] && { warn "Docker daemon not reachable; the stack cannot start."; exit 1; }
  sleep 2
done

# ---------------------------------------------------------------------------
# 1. Best-effort local tooling (NOT required to watch the demo; the whole stack
#    runs in Docker). uv lets a curious participant validate/edit SML rules with
#    osprey-cli; corepack/pnpm lets them run the UI dev server. Failures here are
#    non-fatal and never block the demo.
# ---------------------------------------------------------------------------
log "Installing local tooling (best-effort: uv for rule validation, pnpm for the UI)"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1 || warn "uv install failed (skipping)"
  export PATH="$HOME/.local/bin:$PATH"
fi
if command -v uv >/dev/null 2>&1; then
  (uv sync --dev >/dev/null 2>&1 && ok "uv sync complete (osprey-cli available)") || warn "uv sync failed (skipping)"
else
  warn "uv not on PATH; skipping Python setup"
fi

if command -v corepack >/dev/null 2>&1; then
  corepack enable >/dev/null 2>&1 || true
  ( cd osprey_ui && corepack prepare --activate >/dev/null 2>&1 \
      && pnpm install --frozen-lockfile >/dev/null 2>&1 \
      && ok "pnpm install complete (UI dev server ready)" ) \
    || warn "pnpm install failed (skipping; UI still runs via Docker)"
else
  warn "corepack not found; skipping UI dep install (UI still runs via Docker)"
fi

# ---------------------------------------------------------------------------
# 2. Bring up the stack. Prefer the live Bluesky JetStream path if present.
# ---------------------------------------------------------------------------
wait_for() { # name url max_attempts
  local name="$1" url="$2" max="${3:-90}" i=1
  log "Waiting for $name ($url)"
  while [ "$i" -le "$max" ]; do
    curl -sf "$url" >/dev/null 2>&1 && { ok "$name is ready"; return 0; }
    sleep 2; i=$((i + 1))
  done
  warn "$name did not come up in time; check 'docker compose logs'"
  return 1
}

SOURCE_MODE="synthetic"
if [ -f "run-atproto.sh" ] && [ -f "docker-compose.atproto.yaml" ]; then
  SOURCE_MODE="jetstream"
fi

if [ "$SOURCE_MODE" = "jetstream" ]; then
  log "JetStream files detected -> running Osprey against the LIVE Bluesky firehose"
  log "Pulling images and starting the stack (first run takes several minutes)"
  # run-atproto.sh stacks docker-compose.atproto.yaml on the main compose file.
  # Pass 'up -d --build' so postCreate returns instead of blocking in the foreground.
  bash run-atproto.sh up -d --build
  wait_for "Druid Broker" "http://localhost:8082/status" 180 || true
  wait_for "Osprey UI API" "http://localhost:5004/config" 120 || true
  wait_for "Osprey UI" "http://localhost:5002" 90 || true
  RULES_DIR="example_atproto_rules"
else
  log "No JetStream files on this branch -> using the synthetic producer via demo.sh"
  log "This pulls images, starts the stack, and turns on the test_data producer"
  # demo.sh is non-interactive on a fresh machine (no conflicting containers) and
  # returns once the test_data producer is emitting ~1 event/second.
  bash demo.sh
  RULES_DIR="example_rules"
fi

# ---------------------------------------------------------------------------
# 3. Print clear, non-technical next steps.
# ---------------------------------------------------------------------------
cat <<EOF

============================================================
  Osprey is running.  Event source: ${SOURCE_MODE}
============================================================

OPEN THE DASHBOARD
  In the Codespaces "PORTS" tab, click the globe icon next to port 5002
  (labelled "Osprey UI"). That opens the Osprey dashboard in your browser.

WATCH RULES FIRE
  In the dashboard:
    1. Event Stream  - live events flowing through the engine, with rule matches
    2. TopN panel    - users grouped by the labels/bans rules applied
    3. Query filter  - narrow the stream, e.g. type:  LazyPostRule == True
    4. Rules viz     - the rule dependency graph

EOF

if [ "$SOURCE_MODE" = "jetstream" ]; then
  cat <<EOF
  Source: the LIVE Bluesky JetStream firehose (real posts, ~high volume).
  Active example rule: PostContainsTestRule labels the author 'test-poster'
  when a post contains the word "test". (See ${RULES_DIR}/rules/record/post/.)
EOF
else
  cat <<EOF
  Source: a synthetic producer emitting ~1 event/second. Active example rules:
    - ContainsHello  bans users who say "hello"
    - LazyPostRule   labels posts containing "lazy" as low_effort
    - QuickPostRule  labels posts containing "quick" as potential_bot
    - FoxPostRule    bans users who say "fox"
EOF
fi

cat <<EOF

EDIT A RULE AND SEE IT RELOAD
  Rules are plain text under  ${RULES_DIR}/  (mounted into the worker).
  1. Open a rule file, e.g.  ${RULES_DIR}/rules/  and change a phrase.
  2. Restart the worker to load your change:
         docker compose restart osprey-worker
  3. Watch new events match your edited rule in the Event Stream.

USEFUL COMMANDS
  docker compose ps                 # what's running
  docker compose logs -f osprey-worker   # follow the engine's log
  docker compose down               # stop the demo (keep data)
  docker compose --profile test_data down -v   # stop and wipe (synthetic path)

Other forwarded ports: 5004 UI API, 5001 worker, 8888 Druid console, 9001 MinIO.
============================================================
EOF
