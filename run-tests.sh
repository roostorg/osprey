#!/bin/bash

docker compose -f docker-compose.yaml -f docker-compose.test.yaml -f docker-compose.diag.yaml --profile test run --rm --remove-orphans --entrypoint bash test_runner /osprey/diag.sh "${@}"
