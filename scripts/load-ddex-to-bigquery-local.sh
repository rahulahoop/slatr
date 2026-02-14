#!/bin/bash

# Script to load DDEX ERN XML files into BigQuery emulator using Firebase model
# This demonstrates the full workflow locally

set -e

CONTAINER_NAME="bigquery-emulator-ddex"

# Parse arguments
RUN_TESTS=true
if [ "$1" = "skip-tests" ] || [ "$1" = "no-tests" ]; then
    RUN_TESTS=false
fi

echo "🎵 DDEX to BigQuery Local Example"
echo "=================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Set platform for ARM64 compatibility (Apple Silicon)
export DOCKER_DEFAULT_PLATFORM=linux/amd64

# Pull emulator image if needed
echo "📦 Checking for BigQuery emulator image..."
if ! docker images ghcr.io/goccy/bigquery-emulator:latest | grep -q "latest"; then
    echo "   Pulling BigQuery emulator image (AMD64 for ARM64 compatibility)..."
    docker pull --platform linux/amd64 ghcr.io/goccy/bigquery-emulator:latest
else
    echo "   ✓ Image already exists"
fi

# Clean up any existing container
echo ""
echo "🧹 Cleaning up any existing emulator..."
if docker ps -a | grep -q "$CONTAINER_NAME"; then
    echo "   Stopping and removing existing container..."
    docker stop "$CONTAINER_NAME" 2>/dev/null || true
    docker rm "$CONTAINER_NAME" 2>/dev/null || true
    sleep 2
fi

# Start emulator
echo ""
echo "🚀 Starting BigQuery emulator (AMD64 emulation on ARM64)..."
docker run -d --rm \
    --platform linux/amd64 \
    -p 9050:9050 \
    -p 9060:9060 \
    --name "$CONTAINER_NAME" \
    ghcr.io/goccy/bigquery-emulator:latest \
    --project=test-project \
    --dataset=music_metadata

# Wait for emulator to start
echo "   Waiting for emulator to be ready..."
sleep 8

# Check if emulator is running
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    echo "❌ Error: Emulator failed to start"
    docker logs "$CONTAINER_NAME" 2>/dev/null || true
    exit 1
fi

echo "   ✅ Emulator is running"
echo "   REST API: http://localhost:9050"
echo "   gRPC API: localhost:9060"

# Test connection
echo ""
echo "🔍 Testing emulator connection..."
sleep 2
if curl -s http://localhost:9050 >/dev/null 2>&1; then
    echo "   ✓ REST API responding"
else
    echo "   ⚠️  REST API not responding yet, continuing..."
fi

# Run the integration tests or keep running
if [ "$RUN_TESTS" = true ]; then
    echo ""
    echo "🧪 Running DDEX integration tests..."
    echo ""
    DOCKER_DEFAULT_PLATFORM=linux/amd64 sbt "integrationTests/testOnly *DdexToBigQuerySpec"
    
    TEST_EXIT_CODE=$?
    
    # Stop emulator after tests
    echo ""
    echo "🛑 Stopping BigQuery emulator..."
    docker stop "$CONTAINER_NAME"
    
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
    else
        echo ""
        echo "❌ Tests failed"
        exit 1
    fi
else
    echo ""
    echo "🎯 Emulator is running and ready for queries!"
    echo ""
    echo "📊 How to query the data:"
    echo ""
    echo "   Option 1: Use curl (REST API)"
    echo "   ---------------------------------"
    echo "   curl -X POST http://localhost:9050/bigquery/v2/projects/test-project/jobs \\"
    echo "     -H 'Content-Type: application/json' \\"
    echo "     -d '{\"query\": \"SELECT * FROM \\\`test-project.music_metadata.release_notifications\\\` LIMIT 10\"}'"
    echo ""
    echo "   Option 2: Use Python client"
    echo "   ---------------------------------"
    echo "   python3 scripts/query-bigquery-emulator.py"
    echo ""
    echo "   Option 3: Stop emulator when done"
    echo "   ---------------------------------"
    echo "   docker stop $CONTAINER_NAME"
    echo ""
    echo "💡 The emulator will keep running until you stop it."
    echo "   View logs: docker logs -f $CONTAINER_NAME"
    echo ""
fi
