#!/bin/bash

# Stops the isolated test stack that ./run-tests.sh leaves running.
#
# `-p osprey-test` is load-bearing here for the same reason it is in run-tests.sh, and
# omitting it is worse on this side: without an explicit project name compose derives
# one from the directory (`osprey`), which is the dev stack's project -- so this would
# tear down your running development containers instead of the test ones.
#
# `docker compose run --rm` removes only test_runner, so postgres, kafka, etcd, minio
# and snowflake-id-worker stay up between runs. That is usually what you want, since
# they take tens of seconds to become healthy again and the test database is recreated
# per session regardless. This is for when you want the resources back.
#
# `--volumes` is deliberately not passed: docker-compose.test.yaml resets `volumes` on
# postgres and minio, so their data lives in the container layer and there are no
# `osprey-test_*` volumes to remove. Pass it yourself if that ever stops being true.
docker compose -p osprey-test -f docker-compose.yaml -f docker-compose.test.yaml \
  --profile test down --remove-orphans "${@}"
