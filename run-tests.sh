#!/bin/bash

# `-p osprey-test` is load-bearing. docker-compose.test.yaml clears `container_name`,
# `ports` and `volumes` on the shared services so a test stack can coexist with a
# running dev stack -- but that only works if the two are separate compose *projects*.
# Without an explicit project name, compose derives one from the directory (`osprey`),
# which is the dev stack's project, so the overrides are applied to the dev containers:
# postgres gets recreated with no host port and, worse, no volume. That silently
# destroys the local development database and leaves osprey-ui-api unable to connect.
docker compose -p osprey-test -f docker-compose.yaml -f docker-compose.test.yaml \
  --profile test run --rm -T test_runner run-tests "${@}"
