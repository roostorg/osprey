#!/bin/bash

usage() {
    cat <<EOF
Usage: entrypoint.sh <command>
Osprey docker entrypoint.

Commands:
  osprey-worker
    Runs the worker
  osprey-ui-api
    Runs the Osprey UI API
  run-tests
    Runs tests for the various projects supported here
  operator
    Waits.
EOF
    exit 1
}

cli-osprey-ui-api() {
  exec uv run gunicorn \
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
    exec uv run python3.11 osprey_worker/src/osprey/worker/cli/sinks.py run-rules-sink
}

cli-run-tests() {
  # Only use in CI via harbormaster buildkite run_tests VARIANT PROJECT [directories]
  # Docker command will be run-tests --junitxml=/osprey/junit-pytest.xml [directory]
  # Last argument is the directory, the rest are pytest args
  exec uv run python3.11 -m gevent.monkey --module pytest "${@}"
}

cli-operator() {
  while true; do
      sleep 600
  done
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
