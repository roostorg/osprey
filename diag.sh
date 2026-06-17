#!/usr/bin/env bash
# TEMP DIAGNOSTIC (do not merge): run the suite exactly like the runner
# (python -m gevent.monkey --module pytest) in the background, then an
# out-of-process watchdog (immune to the GIL / gevent stall) dumps the native
# stack + kernel wait-state of every thread if the process is still alive well
# after the suite should have finished.
cd /osprey || exit 1

echo "DIAG: installing py-spy" >&2
uv pip install py-spy >/tmp/pyspy-install.log 2>&1 || echo "DIAG: py-spy install failed (see /tmp/pyspy-install.log)" >&2

echo "DIAG: launching pytest under gevent monkey" >&2
.venv/bin/python3.11 -m gevent.monkey --module pytest "$@" &
PID=$!
echo "DIAG: pytest pid=$PID" >&2

(
  for _ in $(seq 1 36); do
    kill -0 "$PID" 2>/dev/null || { echo "DIAG: process exited on its own (no hang)" >&2; exit 0; }
    sleep 5
  done
  echo "================ DIAG DUMP: pid=$PID still alive after ~180s ================" >&2
  echo "----- per-thread comm / wchan -----" >&2
  for t in /proc/"$PID"/task/*; do
    tid=$(basename "$t")
    printf 'tid=%s comm=%s wchan=%s\n' "$tid" "$(cat "$t/comm" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)" >&2
  done
  echo "----- MAIN thread kernel stack (tid=$PID) -----" >&2
  cat "/proc/$PID/task/$PID/stack" >&2 2>&1 || echo "(kernel stack unreadable)" >&2
  echo "----- py-spy dump --native -----" >&2
  .venv/bin/py-spy dump --pid "$PID" --native >&2 2>&1 || .venv/bin/py-spy dump --pid "$PID" >&2 2>&1 || echo "(py-spy failed)" >&2
  echo "================ DIAG DUMP END; killing pid=$PID ================" >&2
  kill -9 "$PID" 2>/dev/null
) &
WATCH=$!

wait "$PID"
CODE=$?
kill "$WATCH" 2>/dev/null
echo "DIAG: pytest wait returned $CODE" >&2
exit "$CODE"
