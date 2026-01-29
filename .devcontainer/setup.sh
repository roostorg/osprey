#!/bin/bash
set -e

echo "=========================================="
echo "  Osprey Demo - Codespace Setup"
echo "=========================================="

# Wait for Docker to be ready
echo "Waiting for Docker..."
until docker info > /dev/null 2>&1; do
  sleep 1
done
echo "Docker is ready."

# Start services
echo "Starting Osprey services (this may take a few minutes)..."
docker compose pull
docker compose up -d

# Wait for key services
echo "Waiting for services to be healthy..."

echo "  Waiting for Kafka..."
until docker ps | grep osprey-kafka | grep -q healthy 2>/dev/null; do
  sleep 3
done
echo "  Kafka is ready."

echo "  Waiting for Postgres..."
until docker ps | grep postgres | grep -q healthy 2>/dev/null; do
  sleep 3
done
echo "  Postgres is ready."

echo "  Waiting for Druid..."
until curl -s "http://localhost:8888/status" > /dev/null 2>&1; do
  sleep 5
done
echo "  Druid is ready."

echo "  Waiting for UI (this may take a minute for initial build)..."
until curl -s -o /dev/null -w "%{http_code}" "http://localhost:5002/" 2>/dev/null | grep -q "200"; do
  sleep 5
done
echo "  UI is ready."

echo ""
echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""
echo "To start the demo data generator, run:"
echo "  ./example_data/generate_test_data.sh"
echo ""
echo "Then open the Ports tab and click the globe icon"
echo "next to port 5002 to access the UI."
echo ""
