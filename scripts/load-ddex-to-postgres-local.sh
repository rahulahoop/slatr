#!/bin/bash

# Load DDEX ERN XML files into the local PostgreSQL playground.
# The playground must be running: docker compose up -d postgres
#
# Usage:
#   ./scripts/load-ddex-to-postgres-local.sh              # loads examples/out.xml
#   ./scripts/load-ddex-to-postgres-local.sh path/to.xml   # loads a specific file

set -e

DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"
DB_NAME="${PGDATABASE:-music_metadata}"
DB_USER="${PGUSER:-slatr}"
DB_PASS="${PGPASSWORD:-slatr}"
TABLE="${PGTABLE:-ddex_releases}"
XML_FILE="${1:-examples/out.xml}"

echo "PostgreSQL DDEX Loader"
echo "======================"
echo ""
echo "  Host:     $DB_HOST:$DB_PORT"
echo "  Database: $DB_NAME"
echo "  Table:    $TABLE"
echo "  XML file: $XML_FILE"
echo ""

# --- pre-flight checks ---
if [ ! -f "$XML_FILE" ]; then
    echo "Error: XML file not found: $XML_FILE"
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q slatr-postgres; then
    echo "Error: slatr-postgres container is not running."
    echo "Start it with:  docker compose up -d postgres"
    exit 1
fi

# Wait for postgres to be healthy
echo "Checking PostgreSQL is ready..."
for i in $(seq 1 10); do
    if docker exec slatr-postgres pg_isready -U "$DB_USER" -d "$DB_NAME" > /dev/null 2>&1; then
        echo "  PostgreSQL is ready."
        break
    fi
    if [ "$i" -eq 10 ]; then
        echo "Error: PostgreSQL did not become ready in time."
        exit 1
    fi
    sleep 1
done

echo ""
echo "Loading $XML_FILE into $TABLE via sbt..."
echo ""

# Run sbt with a one-off main that invokes PostgreSQLWriter.
# We pass the config as system properties so the Scala code can pick them up.
sbt \
  -Dslatr.pg.host="$DB_HOST" \
  -Dslatr.pg.port="$DB_PORT" \
  -Dslatr.pg.database="$DB_NAME" \
  -Dslatr.pg.user="$DB_USER" \
  -Dslatr.pg.password="$DB_PASS" \
  -Dslatr.pg.table="$TABLE" \
  -Dslatr.pg.xmlFile="$XML_FILE" \
  "core/runMain io.slatr.tools.LoadToPostgres"

echo ""
echo "Done. Query the data with:"
echo "  docker exec -it slatr-postgres psql -U $DB_USER -d $DB_NAME -c \"SELECT * FROM ddex_releases_flat;\""
