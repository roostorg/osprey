#!/usr/bin/env bash
# TEMP DIAGNOSTIC (do not merge): run the suite exactly like the runner
# (uv run python -m gevent.monkey --module pytest) in the background, then an
# out-of-process watchdog (immune to the GIL / gevent stall) dumps the native
# stack + kernel wait-state of every thread if the process is still alive well
# after the suite should have finished.
cd /osprey || exit 1

echo "DIAG: installing py-spy" >&2
uv pip install py-spy >/tmp/pyspy-install.log 2>&1 || echo "DIAG: py-spy install failed (see /tmp/pyspy-install.log)" >&2

echo "DIAG: launching pytest under gevent monkey (uv run)" >&2
uv run python3.11 -m gevent.monkey --module pytest "$@" &
UVPID=$!
echo "DIAG: uv pid=$UVPID" >&2

(
  for _ in $(seq 1 36); do
    kill -0 "$UVPID" 2>/dev/null || { echo "DIAG: process exited on its own (no hang)" >&2; exit 0; }
    sleep 5
  done
  PYPID=$(pgrep -f 'gevent.monkey' | head -1)
  echo "================ DIAG DUMP: still alive ~180s (uv=$UVPID py=$PYPID) ================" >&2
  if [ -n "$PYPID" ]; then
    echo "----- per-thread comm / wchan (py=$PYPID) -----" >&2
    for t in /proc/"$PYPID"/task/*; do
      tid=$(basename "$t")
      printf 'tid=%s comm=%s wchan=%s\n' "$tid" "$(cat "$t/comm" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)" >&2
    done
    echo "----- MAIN thread kernel stack (tid=$PYPID) -----" >&2
    cat "/proc/$PYPID/task/$PYPID/stack" >&2 2>&1 || echo "(kernel stack unreadable)" >&2
  fi
  echo "----- py-spy dump --native --subprocesses -----" >&2
  uv run py-spy dump --pid "$UVPID" --native --subprocesses >&2 2>&1 \
    || uv run py-spy dump --pid "${PYPID:-$UVPID}" --native >&2 2>&1 \
    || echo "(py-spy failed)" >&2
  echo "================ DIAG DUMP END; killing ================" >&2
  kill -9 "$UVPID" 2>/dev/null
  [ -n "$PYPID" ] && kill -9 "$PYPID" 2>/dev/null
) &
WATCH=$!

wait "$UVPID"
CODE=$?
kill "$WATCH" 2>/dev/null
echo "DIAG: wait returned $CODE" >&2
exit "$CODE"
