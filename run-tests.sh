#!/bin/bash
# TEMP DIAGNOSTIC (do not merge): the hang is a flaky gevent-finalization race.
# Loop the EXACT real entrypoint many times; for each iteration a background
# watcher docker-execs py-spy if the container is still alive after the suite
# should be done (i.e. it hung), capturing the native stack, then unblocks it.
set -x
CF="-f docker-compose.yaml -f docker-compose.test.yaml -f docker-compose.diag.yaml --profile test"

docker compose $CF build test_runner

for i in $(seq 1 8); do
  echo "======================== ITERATION $i ========================"
  NAME="osprey_tr_$i"
  (
    sleep 150
    if docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
      echo "WATCHER: $NAME still alive at +150s -> HANG; dumping"
      docker exec "$NAME" bash -lc '
        cd /osprey
        uv pip install py-spy >/tmp/spy.log 2>&1 || echo "py-spy install failed"
        PID=$(pgrep -x python3.11 | head -1)
        [ -z "$PID" ] && PID=$(pgrep -f "python3.11 -m gevent.monkey" | tail -1)
        echo "WATCHER-IN: hung python PID=$PID"
        [ -z "$PID" ] && { echo "WATCHER-IN: no gevent python"; exit 0; }
        echo "----- ps -ef -----"; ps -ef
        echo "----- per-thread comm/wchan -----"
        for t in /proc/$PID/task/*; do tid=$(basename "$t"); printf "tid=%s comm=%s wchan=%s\n" "$tid" "$(cat "$t/comm" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)"; done
        echo "----- main thread (tid=$PID) kernel stack -----"; cat /proc/$PID/task/$PID/stack 2>&1
        echo "----- py-spy dump --native -----"; .venv/bin/py-spy dump --pid $PID --native 2>&1 || .venv/bin/py-spy dump --pid $PID 2>&1 || echo "(py-spy failed)"
      ' 2>&1
      docker kill "$NAME" 2>/dev/null
    fi
  ) &
  W=$!
  timeout 200 docker compose $CF run --rm --name "$NAME" test_runner run-tests
  rc=$?
  kill "$W" 2>/dev/null
  echo "ITER $i rc=$rc"
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then echo "ITER $i HUNG (rc=$rc) -- stopping"; break; fi
done

docker compose $CF down -v --remove-orphans || true
