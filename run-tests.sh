#!/bin/bash
set -e

echo "Starting required services..."
docker compose up -d kafka bigtable bigtable_initializer minio minio-bucket-init postgres snowflake

echo "Waiting for services to be ready..."
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null
docker compose exec postgres pg_isready -U osprey >/dev/null

echo "Running pytest..."
# Set environment variables for test services
export BIGTABLE_EMULATOR_HOST=localhost:8361
export POSTGRES_HOSTS='{"osprey_db":"postgresql://osprey:FoolishPassword@localhost:5432/osprey"}'
export OSPREY_KAFKA_BOOTSTRAP_SERVERS='["localhost:9092"]'
export OSPREY_MINIO_ENDPOINT=localhost:9000
export OSPREY_MINIO_ACCESS_KEY=minioadmin
export OSPREY_MINIO_SECRET_KEY=minioadmin123
export OSPREY_MINIO_SECURE=false
export SNOWFLAKE_API_ENDPOINT=http://localhost:8080
export SNOWFLAKE_EPOCH=1420070400000

# Run pytest with coverage if available
if command -v pytest-cov &>/dev/null; then
    uv run pytest --cov=osprey_worker --cov=osprey_rpc "$@"
else
    uv run pytest "$@"
fi

echo "Stopping services..."
docker compose down
