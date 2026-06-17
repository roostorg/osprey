#!/usr/bin/env bash
# TEMP DIAGNOSTIC (do not merge): run pytest in the FOREGROUND attached to the
# container's stdio (mirroring `exec uv run ... pytest` in entrypoint.sh) and a
# pre-started out-of-process watchdog dumps native stack + kernel wait-state of
# every thread if the gevent python is still alive after the suite should be done.
cd /osprey || exit 1

echo "DIAG: installing py-spy" >&2
uv pip install py-spy >/tmp/pyspy-install.log 2>&1 || echo "DIAG: py-spy install failed" >&2

# Watchdog started BEFORE pytest so it survives a foreground hang.
(
  sleep 200
  PYPID=$(pgrep -f 'gevent.monkey' | head -1)
  if [ -z "$PYPID" ]; then
    echo "DIAG: no gevent python alive at +200s (no hang)" >&2
    exit 0
  fi
  echo "================ DIAG DUMP: py=$PYPID still alive (+200s) ================" >&2
  echo "----- per-thread comm / wchan -----" >&2
  for t in /proc/"$PYPID"/task/*; do
    tid=$(basename "$t")
    printf 'tid=%s comm=%s wchan=%s\n' "$tid" "$(cat "$t/comm" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)" >&2
  done
  echo "----- MAIN thread kernel stack (tid=$PYPID) -----" >&2
  cat "/proc/$PYPID/task/$PYPID/stack" >&2 2>&1 || echo "(kernel stack unreadable)" >&2
  echo "----- py-spy dump --native -----" >&2
  uv run py-spy dump --pid "$PYPID" --native >&2 2>&1 || uv run py-spy dump --pid "$PYPID" >&2 2>&1 || echo "(py-spy failed)" >&2
  echo "================ DIAG DUMP END; killing $PYPID ================" >&2
  kill -9 "$PYPID" 2>/dev/null
) &
WATCH=$!

echo "DIAG: launching pytest in foreground (uv run)" >&2
uv run python3.11 -m gevent.monkey --module pytest "$@"
CODE=$?
echo "DIAG: pytest returned $CODE" >&2
kill "$WATCH" 2>/dev/null
exit "$CODE"
