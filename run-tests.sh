#!/bin/bash

docker compose -f docker-compose.yaml -f docker-compose.test.yaml --profile test run --rm --remove-orphans -T test_runner run-tests "${@}"
