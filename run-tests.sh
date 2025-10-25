#!/bin/bash
set -e

# Function to run docker compose commands with both files
dc() {
    docker compose -f docker-compose.yaml -f docker-compose.test.yaml --profile test "$@"
}

# Cleanup function to run on script exit
cleanup() {
    echo "Cleaning up services..."
    dc down --remove-orphans --volumes
}

# Trap cleanup function on script exit (success or failure)
trap cleanup EXIT

echo "Starting required services..."
dc up -d --force-recreate

echo "Waiting for services to be ready..."

# Wait for Kafka
echo "Waiting for Kafka..."
for i in {1..30}; do
    if dc exec kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
        echo "Kafka is ready"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Kafka failed to start within timeout"
        exit 1
    fi
    sleep 2
done

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
for i in {1..30}; do
    if dc exec postgres pg_isready -U osprey -d osprey >/dev/null 2>&1; then
        echo "PostgreSQL is ready"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "PostgreSQL failed to start within timeout"
        exit 1
    fi
    sleep 2
done

# Wait for MinIO
echo "Waiting for MinIO..."
for i in {1..30}; do
    if dc exec minio curl -f http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        echo "MinIO is ready"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "MinIO failed to start within timeout"
        exit 1
    fi
    sleep 2
done

echo "All services ready. Running tests..."

# Run tests using the test_runner container
dc run --rm test_runner "$@"
