#!/bin/bash

# Load DDEX XML data into a running BigQuery emulator
# Usage: ./scripts/load-data-into-emulator.sh [xml-file]

set -e

XML_FILE="${1:-examples/out.xml}"
EMULATOR_URL="http://localhost:9050"

echo "🎵 Loading DDEX data into BigQuery emulator"
echo "============================================"
echo ""

# Check if emulator is running
if ! curl -s "$EMULATOR_URL" >/dev/null 2>&1; then
    echo "❌ Emulator is not running at $EMULATOR_URL"
    echo ""
    echo "Start it with:"
    echo "  ./scripts/load-ddex-to-bigquery-local.sh skip-tests"
    exit 1
fi

echo "✅ Emulator is running at $EMULATOR_URL"
echo ""

# Check if XML file exists
if [ ! -f "$XML_FILE" ]; then
    echo "❌ XML file not found: $XML_FILE"
    exit 1
fi

echo "📄 Loading XML file: $XML_FILE"
echo ""

# Set environment variables for emulator
export BIGQUERY_EMULATOR_HOST="localhost:9050"

# Run slatr to-bigquery command with Firebase model
echo "🚀 Running slatr to-bigquery..."
echo ""

sbt "cli/run to-bigquery \
  --input $XML_FILE \
  --project-id test-project \
  --dataset-id music_metadata \
  --table-id release_notifications \
  --firebase-model \
  --write-mode overwrite \
  --bigquery-endpoint http://localhost:9050"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ Data loaded successfully!"
    echo ""
    echo "📊 Query the data with:"
    echo "  ./scripts/query-bigquery-curl.sh"
    echo "  python3 scripts/query-bigquery-emulator.py"
else
    echo ""
    echo "❌ Failed to load data (exit code: $EXIT_CODE)"
    exit 1
fi
