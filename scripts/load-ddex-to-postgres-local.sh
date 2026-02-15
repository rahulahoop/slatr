#!/bin/bash

# Load XML files into the local PostgreSQL playground.
# The playground must be running: docker compose up -d postgres
#
# Usage:
#   ./scripts/load-ddex-to-postgres-local.sh                          # load examples/out.xml (firebase)
#   ./scripts/load-ddex-to-postgres-local.sh --all                    # load ALL examples/*.xml (firebase)
#   ./scripts/load-ddex-to-postgres-local.sh --all --traditional      # load ALL examples/*.xml (columnar)
#   ./scripts/load-ddex-to-postgres-local.sh --traditional file.xml   # load one file (columnar)
#   ./scripts/load-ddex-to-postgres-local.sh --all --table my_tbl     # load all into one table
#
# Flags:
#   --all           Load every *.xml in examples/
#   --traditional   Use traditional columnar schema (default: firebase JSONB)
#   --firebase      Use firebase JSONB model (default)
#   --table NAME    Force all files into a single table NAME

set -e

DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"
DB_NAME="${PGDATABASE:-music_metadata}"
DB_USER="${PGUSER:-slatr}"
DB_PASS="${PGPASSWORD:-slatr}"
MODE="firebase"
LOAD_ALL=false
TABLE=""
XML_FILE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --all|-a)
            LOAD_ALL=true
            shift
            ;;
        --traditional|--trad|--columns)
            MODE="traditional"
            shift
            ;;
        --firebase|--jsonb|--fb)
            MODE="firebase"
            shift
            ;;
        --table|-t)
            TABLE="$2"
            shift 2
            ;;
        -*)
            echo "Unknown flag: $1"
            echo "Usage: $0 [--all] [--traditional|--firebase] [--table NAME] [file.xml]"
            exit 1
            ;;
        *)
            XML_FILE="$1"
            shift
            ;;
    esac
done

echo "PostgreSQL XML Loader"
echo "====================="
echo ""
echo "  Host:     $DB_HOST:$DB_PORT"
echo "  Database: $DB_NAME"
echo "  Mode:     $MODE"

# --- pre-flight checks ---
if ! docker ps --format '{{.Names}}' | grep -q slatr-postgres; then
    echo ""
    echo "Error: slatr-postgres container is not running."
    echo "Start it with:  just pg-start"
    exit 1
fi

echo "  Checking PostgreSQL is ready..."
for i in $(seq 1 10); do
    if docker exec slatr-postgres pg_isready -U "$DB_USER" -d "$DB_NAME" > /dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 10 ]; then
        echo "Error: PostgreSQL did not become ready in time."
        exit 1
    fi
    sleep 1
done

# Build sbt -D flags
SBT_PROPS=(
    "-Dslatr.pg.host=$DB_HOST"
    "-Dslatr.pg.port=$DB_PORT"
    "-Dslatr.pg.database=$DB_NAME"
    "-Dslatr.pg.user=$DB_USER"
    "-Dslatr.pg.password=$DB_PASS"
    "-Dslatr.pg.mode=$MODE"
)

if [ -n "$TABLE" ]; then
    SBT_PROPS+=("-Dslatr.pg.table=$TABLE")
fi

if [ "$LOAD_ALL" = true ]; then
    SBT_PROPS+=("-Dslatr.pg.xmlDir=examples")
    echo "  Loading: all *.xml in examples/"
else
    FILE="${XML_FILE:-examples/out.xml}"
    if [ ! -f "$FILE" ]; then
        echo "Error: XML file not found: $FILE"
        exit 1
    fi
    SBT_PROPS+=("-Dslatr.pg.xmlFile=$FILE")
    echo "  Loading: $FILE"
fi

echo ""

sbt "${SBT_PROPS[@]}" "core/runMain io.slatr.tools.LoadToPostgres"
