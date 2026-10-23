#!/bin/bash
# End-to-end smoke test for the demo stack (docs/development/releases.md).
#
# Brings up the stack from docker-compose.yaml, pushes one event rendered from
# example_data/template.json through Kafka ingestion, and asserts over HTTP that
# the example_rules `ContainsHello` rule fired and that the result (feature, ban
# verdict, and `meow` label) is queryable from the UI API.
#
# Usage:
#   ./e2e-smoke.sh              # start the stack, run the test
#   E2E_SKIP_UP=1 ./e2e-smoke.sh  # stack is already running
#
# Requires: docker compose v2, curl, jq. Tear down with `docker compose down -v`.
set -euo pipefail

features=$(mktemp)
trap 'rm -f "$features"' EXIT

UI_API=${UI_API:-http://127.0.0.1:5004}
DRUID_COORDINATOR=${DRUID_COORDINATOR:-http://127.0.0.1:8081}
# Upper bounds for readiness/ingestion; a healthy stack finishes far sooner.
READY_TIMEOUT=${READY_TIMEOUT:-300}
QUERY_TIMEOUT=${QUERY_TIMEOUT:-180}

# Everything the demo needs except the UI frontend, which this test doesn't drive.
SERVICES=(
  osprey-worker osprey-ui-api
  druid-coordinator druid-broker druid-historical druid-middlemanager druid-spec-submitter
)

fail() { echo "FAIL: $*" >&2; exit 1; }

# wait_for <description> <timeout-seconds> <command...>
wait_for() {
  local what=$1 timeout=$2; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || fail "timed out after ${timeout}s waiting for: $what"
    sleep 5
  done
  echo "ok: $what"
}

druid_ingesting() {
  curl -sf "$DRUID_COORDINATOR/druid/indexer/v1/supervisor/osprey.execution_results/status" \
    | jq -e '.payload.activeTasks | length > 0'
}
worker_consuming() { docker compose logs osprey-worker | grep -q 'Updated partition assignment'; }

if [ -z "${E2E_SKIP_UP:-}" ]; then
  docker compose up -d --build "${SERVICES[@]}"
fi

wait_for "UI API healthy" "$READY_TIMEOUT" curl -sf "$UI_API/_health"
wait_for "worker subscribed to Kafka" "$READY_TIMEOUT" worker_consuming
# Druid reads from the latest offset, so don't produce before its task is running.
wait_for "Druid ingestion task running" "$READY_TIMEOUT" druid_ingesting

# One event from the example_data template that the example `ContainsHello` rule matches.
user_id="e2e_$(date +%s)"
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%S.%NZ")
sed -e 's/\$text/hello e2e smoke./' \
    -e "s/\\\$timestamp/$timestamp/" \
    -e "s/\\\$user_id/$user_id/" \
    -e 's/\$ip_address/192.168.1.1/' \
    -e 's/\$action_id/9001/' \
    example_data/template.json \
  | docker compose exec -T osprey-kafka kafka-console-producer \
      --bootstrap-server osprey-kafka:29092 --topic osprey.actions_input
echo "produced event for $user_id"

# Query the result through the UI API (HTTP). Druid ingestion is asynchronous, so poll.
query_event() {
  local start end
  start=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)
  end=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)
  curl -sf -X POST "$UI_API/events/scan" -H 'Content-Type: application/json' \
    -d "{\"start\":\"$start\",\"end\":\"$end\",\"query_filter\":\"UserId == '$user_id'\",\"entity\":null,\"limit\":10}" \
    | jq -e '.events[0].extracted_features' > "$features"
}
wait_for "event queryable via UI API /events/scan" "$QUERY_TIMEOUT" query_event

jq -e '.ContainsHello == true' "$features" >/dev/null \
  || fail "ContainsHello rule did not fire: $(cat "$features")"
jq -e '.__ban_user | index("'"$user_id"'|User said \"hello\"")' "$features" >/dev/null \
  || fail "expected BanUser verdict missing: $(cat "$features")"
jq -e '.__entity_label_mutations | index("User/meow/1")' "$features" >/dev/null \
  || fail "expected User/meow label mutation missing: $(cat "$features")"

echo "PASS: ContainsHello fired, ban verdict + meow label queryable via UI API"
