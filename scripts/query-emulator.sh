#!/bin/bash

# Simple query tool for BigQuery emulator
# Usage: ./scripts/query-emulator.sh "SELECT * FROM \`test-project.music_metadata.sample_releases\`"

EMULATOR_URL="http://localhost:9050"
PROJECT="test-project"

if [ $# -eq 0 ]; then
    echo "Usage: $0 \"YOUR SQL QUERY\""
    echo ""
    echo "Examples:"
    echo "  $0 \"SELECT COUNT(*) FROM \\\`test-project.music_metadata.sample_releases\\\`\""
    echo "  $0 \"SELECT * FROM \\\`test-project.music_metadata.sample_releases\\\` LIMIT 5\""
    exit 1
fi

QUERY="$1"

echo "🔍 Querying BigQuery Emulator"
echo "Query: $QUERY"
echo ""

RESULT=$(curl -s -X POST "$EMULATOR_URL/bigquery/v2/projects/$PROJECT/queries" \
  -H 'Content-Type: application/json' \
  -d "{\"query\": \"$QUERY\", \"useLegacySql\": false}")

# Check for errors
if echo "$RESULT" | jq -e '.error' > /dev/null 2>&1; then
    echo "❌ Query failed:"
    echo "$RESULT" | jq '.error'
    exit 1
fi

# Display results in a table format
echo "📊 Results:"
echo ""

# Get column names
COLUMNS=$(echo "$RESULT" | jq -r '.schema.fields[].name' | paste -sd '|' -)
echo "$COLUMNS"
echo "$(echo "$COLUMNS" | sed 's/[^|]/-/g')"

# Get rows
echo "$RESULT" | jq -r '.rows[]?.f | map(.v) | @tsv' | tr '\t' '|'

echo ""
TOTAL=$(echo "$RESULT" | jq -r '.totalRows')
echo "Total rows: $TOTAL"
