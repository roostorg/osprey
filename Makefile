SHELL := /bin/bash

.DEFAULT_GOAL := help

BASE_PROJECT := osprey
BASE_COMPOSE := docker compose -p $(BASE_PROJECT) -f docker-compose.yaml
COORDINATOR_COMPOSE := docker compose -f docker-compose.yaml -f example_docker_compose/run_osprey_with_coordinator/docker-compose.coordinator.yaml
CLICKHOUSE_DEV_PROJECT := osprey-clickhouse
CLICKHOUSE_DEV_COMPOSE := docker compose -p $(CLICKHOUSE_DEV_PROJECT) -f docker-compose.yaml -f docker-compose.clickhouse.yaml --profile clickhouse
TEST_PROJECT := osprey-test
TEST_COMPOSE := docker compose -p $(TEST_PROJECT) -f docker-compose.yaml -f docker-compose.test.yaml --profile test
DRUID_TEST_PROJECT := osprey-druid-test
DRUID_TEST_COMPOSE := docker compose -p $(DRUID_TEST_PROJECT) -f docker-compose.yaml -f docker-compose.test.yaml -f docker-compose.test.druid.yaml --profile test
CLICKHOUSE_TEST_PROJECT := osprey-clickhouse-test
CLICKHOUSE_TEST_COMPOSE := docker compose -p $(CLICKHOUSE_TEST_PROJECT) -f docker-compose.yaml -f docker-compose.test.yaml -f docker-compose.test.clickhouse.yaml --profile test --profile clickhouse
FETCH_SERVICES := osprey-kafka minio minio-bucket-init postgres snowflake-id-worker druid-zookeeper druid-coordinator druid-broker druid-historical druid-middlemanager druid-router druid-spec-submitter
CLICKHOUSE_FETCH_SERVICES := osprey-kafka minio minio-bucket-init postgres snowflake-id-worker clickhouse
TEST_FETCH_SERVICES := osprey-kafka minio minio-bucket-init postgres snowflake-id-worker etcd
DRUID_TEST_FETCH_SERVICES := osprey-kafka minio minio-bucket-init postgres snowflake-id-worker etcd druid-zookeeper druid-coordinator druid-broker druid-historical druid-middlemanager druid-router druid-spec-submitter
CLICKHOUSE_TEST_FETCH_SERVICES := osprey-kafka minio minio-bucket-init postgres snowflake-id-worker clickhouse

PYTEST_ARGS ?=
DRUID_TEST_ARGS_DEFAULT := osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_druid_integration.py
CLICKHOUSE_TEST_ARGS_DEFAULT := osprey_worker/src/osprey/worker/ui_api/osprey/lib/tests/test_clickhouse.py osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_clickhouse_integration.py

.PHONY: help install hooks lint format format-check typecheck precommit check \
	fetch clickhouse-fetch test-fetch druid-test-fetch clickhouse-test-fetch \
	default-clean clickhouse-clean test-clean druid-test-clean clickhouse-test-clean docker-clean \
	up down reset coordinator-up coordinator-down coordinator-reset \
	seed-data seed-data-down bigtable-up bigtable-down clickhouse-up clickhouse-down clickhouse-reset \
	clickhouse-seed-data clickhouse-seed-data-down test test-down test-reset test-fresh \
	druid-test druid-test-down druid-test-reset druid-test-fresh \
	clickhouse-test clickhouse-test-down clickhouse-test-reset clickhouse-test-fresh

help:
	@echo "Common targets:"
	@echo "  make install                 Install Python dependencies with uv"
	@echo "  make hooks                   Install pre-commit hooks"
	@echo "  make lint                    Run ruff checks"
	@echo "  make format                  Run ruff formatter"
	@echo "  make format-check            Check formatting without rewriting files"
	@echo "  make typecheck               Run mypy"
	@echo "  make precommit               Run pre-commit on all files"
	@echo "  make check                   Run lint, format-check, and typecheck"
	@echo ""
	@echo "  make docker-clean            Remove all repo-scoped Docker stacks, stale profile services, volumes, and images"
	@echo "  make fetch                   Pull/build images for the default dev stack"
	@echo "  make clickhouse-fetch        Pull/build images for the ClickHouse dev stack"
	@echo "  make test-fetch             Pull/build images for the default Docker test stack"
	@echo "  make druid-test-fetch       Pull/build images for the isolated Druid integration test stack"
	@echo "  make clickhouse-test-fetch  Pull/build images for the ClickHouse Docker test stack"
	@echo ""
	@echo "  make up                      Start the default dev stack"
	@echo "  make down                    Stop the default dev stack"
	@echo "  make reset                   Stop the default dev stack and remove volumes"
	@echo "  make coordinator-up          Start the coordinator dev stack"
	@echo "  make coordinator-down        Stop the coordinator dev stack"
	@echo "  make coordinator-reset       Stop the coordinator dev stack and remove volumes"
	@echo "  make bigtable-up            Start the optional Bigtable emulator services"
	@echo "  make bigtable-down          Stop the optional Bigtable emulator services"
	@echo "  make seed-data               Start the default test-data producer"
	@echo "  make seed-data-down          Stop the default test-data producer"
	@echo ""
	@echo "  make clickhouse-up           Start the ClickHouse dev stack"
	@echo "  make clickhouse-down         Stop the ClickHouse dev stack"
	@echo "  make clickhouse-reset        Stop the ClickHouse dev stack and remove volumes"
	@echo "  make clickhouse-seed-data    Start the ClickHouse test-data producer"
	@echo "  make clickhouse-seed-data-down Stop the ClickHouse test-data producer"
	@echo ""
	@echo "  make test                    Run the fast default Docker test runner (no Druid/ClickHouse integration stack)"
	@echo "  make test-down               Stop the default Docker test stack"
	@echo "  make test-reset              Stop the default Docker test stack and remove volumes"
	@echo "  make test-fresh             Clean all Osprey Docker state, run default tests, then clean test state again"
	@echo "  make druid-test              Run the isolated Druid integration test runner"
	@echo "  make druid-test-down         Stop the isolated Druid integration test stack"
	@echo "  make druid-test-reset        Stop the isolated Druid integration test stack and remove volumes"
	@echo "  make druid-test-fresh       Clean all Osprey Docker state, run Druid integration tests, then clean test state again"
	@echo "  make clickhouse-test         Run the isolated ClickHouse test runner"
	@echo "  make clickhouse-test-down    Stop the isolated ClickHouse test stack"
	@echo "  make clickhouse-test-reset   Stop the isolated ClickHouse test stack and remove volumes"
	@echo "  make clickhouse-test-fresh  Clean all Osprey Docker state, run ClickHouse tests, then clean test state again"
	@echo ""
	@echo "Arguments:"
	@echo "  PYTEST_ARGS='path/to/test.py -k expression'  Pass pytest args to make test or make clickhouse-test"

install:
	uv sync

hooks:
	uv run pre-commit install

lint:
	uv run ruff check

format:
	uv run ruff format

format-check:
	uv run ruff format --check

typecheck:
	uv run mypy .

precommit:
	uv run pre-commit run --all-files

check: lint format-check typecheck

fetch:
	$(BASE_COMPOSE) pull --ignore-buildable $(FETCH_SERVICES)
	$(BASE_COMPOSE) build

clickhouse-fetch:
	$(CLICKHOUSE_DEV_COMPOSE) pull --ignore-buildable $(CLICKHOUSE_FETCH_SERVICES)
	$(CLICKHOUSE_DEV_COMPOSE) build

test-fetch:
	$(TEST_COMPOSE) pull --ignore-buildable $(TEST_FETCH_SERVICES)
	$(TEST_COMPOSE) build test_runner

druid-test-fetch:
	$(DRUID_TEST_COMPOSE) pull --ignore-buildable $(DRUID_TEST_FETCH_SERVICES)
	$(DRUID_TEST_COMPOSE) build test_runner

clickhouse-test-fetch:
	$(CLICKHOUSE_TEST_COMPOSE) pull --ignore-buildable $(CLICKHOUSE_TEST_FETCH_SERVICES)
	$(CLICKHOUSE_TEST_COMPOSE) build test_runner

default-clean:
	-$(BASE_COMPOSE) down --remove-orphans --volumes --rmi all
	-$(BASE_COMPOSE) --profile bigtable down --remove-orphans --volumes --rmi all
	-$(BASE_COMPOSE) --profile test_data down --remove-orphans --volumes --rmi all

clickhouse-clean:
	-$(CLICKHOUSE_DEV_COMPOSE) down --remove-orphans --volumes --rmi all
	-$(CLICKHOUSE_DEV_COMPOSE) --profile test_data down --remove-orphans --volumes --rmi all

test-clean:
	-$(TEST_COMPOSE) down --remove-orphans --volumes --rmi all

druid-test-clean:
	-$(DRUID_TEST_COMPOSE) down --remove-orphans --volumes --rmi all

clickhouse-test-clean:
	-$(CLICKHOUSE_TEST_COMPOSE) down --remove-orphans --volumes --rmi all

docker-clean: default-clean clickhouse-clean test-clean druid-test-clean clickhouse-test-clean

up:
	$(BASE_COMPOSE) up -d

down:
	$(BASE_COMPOSE) down --remove-orphans

reset:
	$(BASE_COMPOSE) down --remove-orphans --volumes

coordinator-up:
	$(COORDINATOR_COMPOSE) up -d

coordinator-down:
	$(COORDINATOR_COMPOSE) down --remove-orphans

coordinator-reset:
	$(COORDINATOR_COMPOSE) down --remove-orphans --volumes

seed-data:
	-$(BASE_COMPOSE) --profile test_data rm -sf osprey-kafka-test-data-producer
	$(BASE_COMPOSE) --profile test_data up -d osprey-kafka-test-data-producer

seed-data-down:
	$(BASE_COMPOSE) --profile test_data rm -sf osprey-kafka-test-data-producer

bigtable-up:
	$(BASE_COMPOSE) --profile bigtable up -d bigtable bigtable-initializer

bigtable-down:
	-$(BASE_COMPOSE) --profile bigtable rm -sf bigtable-initializer bigtable

clickhouse-up:
	$(CLICKHOUSE_DEV_COMPOSE) up -d

clickhouse-down:
	$(CLICKHOUSE_DEV_COMPOSE) down --remove-orphans

clickhouse-reset:
	$(CLICKHOUSE_DEV_COMPOSE) down --remove-orphans --volumes

clickhouse-seed-data:
	-$(CLICKHOUSE_DEV_COMPOSE) --profile test_data rm -sf osprey-kafka-test-data-producer
	$(CLICKHOUSE_DEV_COMPOSE) --profile test_data up -d osprey-kafka-test-data-producer

clickhouse-seed-data-down:
	$(CLICKHOUSE_DEV_COMPOSE) --profile test_data rm -sf osprey-kafka-test-data-producer

test:
	$(TEST_COMPOSE) run --rm --remove-orphans test_runner run-tests $(PYTEST_ARGS)

test-down:
	$(TEST_COMPOSE) down --remove-orphans

test-reset:
	$(TEST_COMPOSE) down --remove-orphans --volumes

test-fresh:
	@set -euo pipefail; \
	trap '$(MAKE) test-reset >/dev/null 2>&1 || true' EXIT; \
	$(MAKE) docker-clean; \
	$(MAKE) test PYTEST_ARGS="$(PYTEST_ARGS)"

druid-test:
	$(DRUID_TEST_COMPOSE) run --rm --remove-orphans test_runner run-tests $(if $(strip $(PYTEST_ARGS)),$(PYTEST_ARGS),$(DRUID_TEST_ARGS_DEFAULT))

druid-test-down:
	$(DRUID_TEST_COMPOSE) down --remove-orphans

druid-test-reset:
	$(DRUID_TEST_COMPOSE) down --remove-orphans --volumes

druid-test-fresh:
	@set -euo pipefail; \
	trap '$(MAKE) druid-test-reset >/dev/null 2>&1 || true' EXIT; \
	$(MAKE) docker-clean; \
	$(MAKE) druid-test PYTEST_ARGS="$(PYTEST_ARGS)"

clickhouse-test:
	$(CLICKHOUSE_TEST_COMPOSE) run --rm --remove-orphans test_runner run-tests $(if $(strip $(PYTEST_ARGS)),$(PYTEST_ARGS),$(CLICKHOUSE_TEST_ARGS_DEFAULT))

clickhouse-test-down:
	$(CLICKHOUSE_TEST_COMPOSE) down --remove-orphans

clickhouse-test-reset:
	$(CLICKHOUSE_TEST_COMPOSE) down --remove-orphans --volumes

clickhouse-test-fresh:
	@set -euo pipefail; \
	trap '$(MAKE) clickhouse-test-reset >/dev/null 2>&1 || true' EXIT; \
	$(MAKE) docker-clean; \
	$(MAKE) clickhouse-test PYTEST_ARGS="$(PYTEST_ARGS)"
