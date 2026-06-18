#!/usr/bin/env bash
# TEMP DIAGNOSTIC sidecar (do not merge): runs in a container sharing the
# test_runner PID namespace; dumps the native stack + kernel wait-state of the
# hung gevent python without perturbing it.
set +e
cd /osprey || exit 1
uv pip install py-spy >/tmp/spy-install.log 2>&1 || echo "SIDECAR: py-spy install failed" >&2

echo "SIDECAR: waiting for gevent python..."
PID=""
for _ in $(seq 1 120); do
  PID=$(pgrep -f 'gevent.monkey' | head -1)
  [ -n "$PID" ] && break
  sleep 2
done
echo "SIDECAR: target gevent python PID=$PID; waiting ~210s into post-suite window"
sleep 210

PID=$(pgrep -f 'gevent.monkey' | head -1)
if [ -z "$PID" ]; then
  echo "SIDECAR: no gevent python alive (no hang reproduced)"
  exit 0
fi

echo "================ SIDECAR DUMP: PID=$PID ================"
echo "----- ps -ef -----"
ps -ef
echo "----- per-thread comm/wchan -----"
for t in /proc/"$PID"/task/*; do
  tid=$(basename "$t")
  printf 'tid=%s comm=%s wchan=%s\n' "$tid" "$(cat "$t/comm" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)"
done
echo "----- main thread (tid=$PID) kernel stack -----"
cat /proc/"$PID"/task/"$PID"/stack 2>&1
echo "----- py-spy dump --native -----"
.venv/bin/py-spy dump --pid "$PID" --native 2>&1 || .venv/bin/py-spy dump --pid "$PID" 2>&1 || echo "(py-spy failed)"
echo "================ SIDECAR DUMP END ================"
kill -9 "$PID" 2>/dev/null
exit 0
