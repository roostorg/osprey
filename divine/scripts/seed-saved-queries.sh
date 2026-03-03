#!/usr/bin/env bash
# Seed default saved queries into the Osprey Postgres database.
# Usage: ./divine/scripts/seed-saved-queries.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Seeding default saved queries into Postgres..."
docker exec -i divine-postgres psql -U osprey -d osprey < scripts/seed-saved-queries.sql
echo "Done — seeded default saved queries."
