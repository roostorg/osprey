#!/bin/bash

usage() {
    cat <<EOF
Usage: entrypoint.sh <command>
Divine Osprey docker entrypoint.

Commands:
  osprey-worker
    Runs the worker
  osprey-ui-api
    Runs the Osprey UI API
EOF
    exit 1
}

cli-osprey-ui-api() {
  exec gunicorn \
    --reload \
    --access-logfile - \
    --error-logfile - \
    --logger-class jslog4kube.GunicornLogger \
    --name osprey_ui_api \
    --worker-class gevent \
    --chdir /osprey/osprey_worker/src \
    --bind :5004 \
    "osprey.worker.ui_api.osprey.app:create_app()"
}

cli-osprey-worker() {
    exec python3.11 osprey_worker/src/osprey/worker/cli/sinks.py run-rules-sink
}

cmd="${1:-}"
case "${cmd}" in
"" | "-h" | "--help")
    usage
    ;;
*)
    if [[ "$(type -t "cli-${cmd}")" = "function" ]]; then
        shift
        "cli-${cmd}" "$@"
    else
        echo "Unknown command: ${cmd}"
        echo ""
        usage
    fi
    ;;
esac
