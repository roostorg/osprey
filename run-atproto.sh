#!/bin/bash
# Run Osprey against the live Bluesky JetStream firehose by stacking
# docker-compose.atproto.yaml on top of the main compose file.
set -e
exec docker compose -f docker-compose.yaml -f docker-compose.atproto.yaml "${@:-up}"
