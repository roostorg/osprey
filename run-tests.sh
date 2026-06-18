#!/bin/bash
# TEMP DIAGNOSTIC (do not merge): run the EXACT real entrypoint via
# `docker compose run` in the FOREGROUND (the config that hung 4/4), and a
# background watcher `docker exec`s py-spy into the container at +220s -- after
# the shutdown hang has set in, so it cannot perturb it.
set -x
CF="-f docker-compose.yaml -f docker-compose.test.yaml -f docker-compose.diag.yaml --profile test"

docker compose $CF build test_runner

(
  sleep 220
  echo "================ WATCHER: docker exec dump of osprey_tr ================"
  docker exec osprey_tr bash -lc '
    cd /osprey
    uv pip install py-spy >/tmp/spy.log 2>&1 || echo "WATCHER-IN: py-spy install failed"
    PID=$(pgrep -f gevent.monkey | head -1)
    echo "WATCHER-IN: gevent python PID=$PID"
    if [ -z "$PID" ]; then echo "WATCHER-IN: no gevent python (no hang)"; exit 0; fi
    echo "----- ps -ef -----"; ps -ef
    echo "----- per-thread comm/wchan -----"
    for t in /proc/$PID/task/*; do tid=$(basename "$t"); printf "tid=%s comm=%s wchan=%s\n" "$tid" "$(cat "$t/comm" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)"; done
    echo "----- main thread (tid=$PID) kernel stack -----"; cat /proc/$PID/task/$PID/stack 2>&1
    echo "----- py-spy dump --native -----"; .venv/bin/py-spy dump --pid $PID --native 2>&1 || .venv/bin/py-spy dump --pid $PID 2>&1 || echo "(py-spy failed)"
  ' 2>&1
  echo "================ WATCHER: killing osprey_tr to unblock ================"
  docker kill osprey_tr 2>/dev/null
) &
WATCHER=$!

docker compose $CF run --rm --name osprey_tr test_runner run-tests
echo "DIAG: foreground run returned $?"

kill "$WATCHER" 2>/dev/null
docker compose $CF down -v --remove-orphans || true
