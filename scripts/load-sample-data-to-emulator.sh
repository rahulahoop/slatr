#!/bin/bash

# Load sample data into the running BigQuery emulator
# This creates a table you can query with curl or Python

set -e

EMULATOR_URL="http://localhost:9050"
PROJECT="test-project"
DATASET="music_metadata"

echo "🎵 Loading Sample Data into BigQuery Emulator"
echo "=============================================="
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

# Create a table with Firebase model schema
echo "📋 Creating table 'sample_releases' with Firebase schema..."

TABLE_SCHEMA='{
  "tableReference": {
    "projectId": "test-project",
    "datasetId": "music_metadata",
    "tableId": "sample_releases"
  },
  "schema": {
    "fields": [
      {
        "name": "fields",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
          {
            "name": "name",
            "type": "STRING",
            "mode": "NULLABLE"
          },
          {
            "name": "value",
            "type": "STRING",
            "mode": "NULLABLE"
          }
        ]
      }
    ]
  }
}'

# Create table
curl -s -X POST "$EMULATOR_URL/bigquery/v2/projects/$PROJECT/datasets/$DATASET/tables" \
  -H 'Content-Type: application/json' \
  -d "$TABLE_SCHEMA" > /dev/null

echo "   ✅ Table created"
echo ""

# Insert sample data
echo "💾 Inserting sample data..."

SAMPLE_DATA='{
  "rows": [
    {
      "json": {
        "fields": [
          {"name": "MessageId", "value": "MSG-001"},
          {"name": "ISRC", "value": "USAT20001234"},
          {"name": "Title", "value": "Sample Song 1"},
          {"name": "Artist", "value": "Test Artist"},
          {"name": "Genre", "value": "Rock"}
        ]
      }
    },
    {
      "json": {
        "fields": [
          {"name": "MessageId", "value": "MSG-002"},
          {"name": "ISRC", "value": "USAT20005678"},
          {"name": "Title", "value": "Sample Song 2"},
          {"name": "Artist", "value": "Another Artist"},
          {"name": "Genre", "value": "Jazz"}
        ]
      }
    },
    {
      "json": {
        "fields": [
          {"name": "MessageId", "value": "MSG-003"},
          {"name": "ISRC", "value": "USAT20009999"},
          {"name": "Title", "value": "Sample Song 3"},
          {"name": "Artist", "value": "Third Artist"},
          {"name": "Genre", "value": "Electronic"},
          {"name": "Year", "value": "2024"}
        ]
      }
    }
  ]
}'

curl -s -X POST "$EMULATOR_URL/bigquery/v2/projects/$PROJECT/datasets/$DATASET/tables/sample_releases/insertAll" \
  -H 'Content-Type: application/json' \
  -d "$SAMPLE_DATA" > /dev/null

echo "   ✅ Inserted 3 sample rows"
echo ""

# Verify data
echo "🔍 Verifying data..."
QUERY='{"query": "SELECT COUNT(*) as count FROM `test-project.music_metadata.sample_releases`", "useLegacySql": false}'

RESULT=$(curl -s -X POST "$EMULATOR_URL/bigquery/v2/projects/$PROJECT/queries" \
  -H 'Content-Type: application/json' \
  -d "$QUERY")

COUNT=$(echo "$RESULT" | jq -r '.rows[0].f[0].v')

echo "   Total rows: $COUNT"
echo ""

echo "✅ Sample data loaded successfully!"
echo ""
echo "📊 Now you can query the data:"
echo ""
echo "   Example 1: Count rows"
echo "   ---------------------"
echo "   curl -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"query\": \"SELECT COUNT(*) FROM \\\`test-project.music_metadata.sample_releases\\\`\", \"useLegacySql\": false}' | jq '.rows'"
echo ""
echo "   Example 2: Get all data"
echo "   -----------------------"
echo "   curl -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"query\": \"SELECT * FROM \\\`test-project.music_metadata.sample_releases\\\` LIMIT 10\", \"useLegacySql\": false}' | jq '.'"
echo ""
echo "   Example 3: Extract specific fields (NOTE: UNNEST may not work in emulator)"
echo "   --------------------------------------------------------------------------"
echo "   curl -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"query\": \"SELECT (SELECT value FROM UNNEST(fields) WHERE name = '\\''ISRC'\\'' LIMIT 1) as isrc FROM \\\`test-project.music_metadata.sample_releases\\\`\", \"useLegacySql\": false}' | jq '.rows'"
echo ""
echo "💡 Tip: The emulator keeps running. To stop it:"
echo "   docker stop bigquery-emulator-ddex"
echo ""
