#!/bin/sh

echo "Waiting for Coordinator-Overlord to be ready..."
while ! curl -f http://coordinator:8081/status >/dev/null 2>&1; do
  sleep 5
done
echo "Coordinator-Overlord is ready!"

echo "Submitting ingestion specs..."
echo "Files in /specs:"
ls -la /specs/

for spec in /specs/*.json; do
  if [ -f "$spec" ]; then
    echo "=== Submitting $spec ==="

    curl -X POST \
      -H "Content-Type: application/json" \
      -d @"$spec" \
      -v \
      http://coordinator:8081/druid/indexer/v1/supervisor

    echo ""
    echo "========================="
    sleep 2
  else
    echo "No JSON files found in /specs/"
  fi
done

echo "Done submitting specs."
