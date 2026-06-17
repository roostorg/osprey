#!/usr/bin/env bash
# TEMP DIAGNOSTIC (do not merge): reproduce the REAL entrypoint (exec uv run
# ... pytest, so the python chain runs as PID 1) while a pre-forked watchdog
# (survives the exec as a separate process) dumps native stack + kernel
# wait-state of every thread if the process is still alive after the suite.
cd /osprey || exit 1

uv pip install py-spy >/tmp/pyspy-install.log 2>&1 || echo "DIAG: py-spy install failed" >&2

(
  sleep 200
  PYPID=$(pgrep -f 'gevent.monkey' | head -1)
  if [ -z "$PYPID" ]; then
    echo "DIAG: no gevent python alive at +200s (no hang)" >&2
    exit 0
  fi
  echo "================ DIAG DUMP: py=$PYPID still alive (+200s) ================" >&2
  echo "----- ps -ef (process tree / who is PID 1) -----" >&2
  ps -ef >&2 2>&1 || true
  echo "----- per-thread comm / wchan (py=$PYPID) -----" >&2
  for t in /proc/"$PYPID"/task/*; do
    tid=$(basename "$t")
    printf 'tid=%s comm=%s wchan=%s\n' "$tid" "$(cat "$t/comm" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)" >&2
  done
  echo "----- MAIN thread kernel stack (tid=$PYPID) -----" >&2
  cat "/proc/$PYPID/task/$PYPID/stack" >&2 2>&1 || echo "(kernel stack unreadable)" >&2
  echo "----- py-spy dump --native -----" >&2
  .venv/bin/py-spy dump --pid "$PYPID" --native >&2 2>&1 || .venv/bin/py-spy dump --pid "$PYPID" >&2 2>&1 || echo "(py-spy failed)" >&2
  echo "================ DIAG DUMP END; killing $PYPID ================" >&2
  kill -9 "$PYPID" 2>/dev/null
) &

echo "DIAG: exec uv run pytest (PID 1 chain, like entrypoint)" >&2
exec uv run python3.11 -m gevent.monkey --module pytest "$@"
