#!/bin/bash

# Query BigQuery emulator using curl (no dependencies needed!)
# Usage: ./scripts/query-bigquery-curl.sh

EMULATOR_URL="http://localhost:9050"
PROJECT="test-project"
DATASET="music_metadata"

echo "🎵 BigQuery Emulator Query Tool (curl)"
echo "======================================"
echo ""

# Check if emulator is running
if ! curl -s "$EMULATOR_URL" >/dev/null 2>&1; then
    echo "❌ Emulator is not running at $EMULATOR_URL"
    echo ""
    echo "Start it with:"
    echo "  ./scripts/load-ddex-to-bigquery-local.sh skip-tests"
    exit 1
fi

echo "✅ Connected to emulator at $EMULATOR_URL"
echo ""

# Function to run a query
run_query() {
    local query="$1"
    local description="$2"
    
    echo "🔍 $description"
    echo "Query: $query"
    echo ""
    
    curl -s -X POST "$EMULATOR_URL/bigquery/v2/projects/$PROJECT/queries" \
        -H 'Content-Type: application/json' \
        -d "{
            \"query\": \"$query\",
            \"useLegacySql\": false
        }" | jq -r '
        if .rows then
            (.schema.fields | map(.name) | @tsv),
            (.rows[] | .f | map(.v) | @tsv)
        else
            "No results or error: " + (.error.message // "Unknown error")
        end
    ' 2>/dev/null || echo "Error running query (jq not installed?)"
    
    echo ""
    echo "---"
    echo ""
}

# Example queries

echo "📊 Example Queries:"
echo ""

# 1. Count rows
run_query \
    "SELECT COUNT(*) as count FROM \\\`$PROJECT.$DATASET.release_notifications\\\`" \
    "Count total rows"

# 2. Show distinct field names
run_query \
    "SELECT DISTINCT field.name as field_name FROM \\\`$PROJECT.$DATASET.release_notifications\\\`, UNNEST(fields) AS field ORDER BY field_name LIMIT 10" \
    "First 10 field names"

# 3. Extract music metadata
run_query \
    "SELECT (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc, (SELECT value FROM UNNEST(fields) WHERE name LIKE '%TitleText%' LIMIT 1) as title FROM \\\`$PROJECT.$DATASET.release_notifications\\\` LIMIT 3" \
    "Music metadata (ISRC and Title)"

echo ""
echo "💡 Custom Queries:"
echo "   Use the run_query function in this script or query directly with curl:"
echo ""
echo "   curl -X POST $EMULATOR_URL/bigquery/v2/projects/$PROJECT/queries \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"query\": \"YOUR_SQL_HERE\", \"useLegacySql\": false}' | jq"
echo ""
