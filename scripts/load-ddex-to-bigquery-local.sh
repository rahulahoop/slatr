#!/bin/bash

# Script to load DDEX ERN XML files into BigQuery emulator using Firebase model
# This demonstrates the full workflow locally

set -e

echo "🎵 DDEX to BigQuery Local Example"
echo "=================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Pull emulator image if needed
echo "📦 Checking for BigQuery emulator image..."
if ! docker images | grep -q "bigquery-emulator"; then
    echo "   Pulling BigQuery emulator image..."
    docker pull ghcr.io/goccy/bigquery-emulator:latest
fi

# Start emulator
echo ""
echo "🚀 Starting BigQuery emulator..."
docker run -d --rm \
    -p 9050:9050 \
    -p 9060:9060 \
    --name bigquery-emulator-ddex \
    ghcr.io/goccy/bigquery-emulator:latest \
    --project=test-project \
    --dataset=music_metadata

# Wait for emulator to start
echo "   Waiting for emulator to be ready..."
sleep 5

# Check if emulator is running
if ! docker ps | grep -q bigquery-emulator-ddex; then
    echo "❌ Error: Emulator failed to start"
    exit 1
fi

echo "   ✅ Emulator is running"
echo "   REST API: http://localhost:9050"
echo "   gRPC API: localhost:9060"

# Run the integration tests
echo ""
echo "🧪 Running DDEX integration tests..."
echo ""
sbt "integrationTests/testOnly *DdexToBigQuerySpec"

TEST_EXIT_CODE=$?

# Stop emulator
echo ""
echo "🛑 Stopping BigQuery emulator..."
docker stop bigquery-emulator-ddex

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ All tests passed!"
    echo ""
    echo "📚 What was tested:"
    echo "   - Loaded out.xml and bout.xml (DDEX ERN format)"
    echo "   - Used Firebase model (array of key-value structs)"
    echo "   - Discovered all field names in the XML"
    echo "   - Extracted music metadata (ISRC, title, artist, etc.)"
    echo "   - Demonstrated schema evolution capabilities"
    echo ""
    echo "🔍 To run manually:"
    echo "   1. Start emulator: just start-emulator"
    echo "   2. Load data:"
    echo "      slatr to-bigquery out.xml -p test-project -d music_metadata -t releases --firebase-model"
    echo "      slatr to-bigquery bout.xml -p test-project -d music_metadata -t releases --firebase-model"
    echo "   3. Query: Use BigQuery SQL at http://localhost:9050"
else
    echo ""
    echo "❌ Tests failed"
    exit 1
fi
