#!/bin/bash

# Script to test DDEX ERN XML loading into PostgreSQL
# Uses Testcontainers to automatically manage PostgreSQL instance

set -e

echo "🐘 PostgreSQL Integration Test"
echo "================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

echo "🚀 Running PostgreSQL integration tests..."
echo "   (Testcontainers will automatically start PostgreSQL)"
echo ""

# Run the integration tests
sbt "integrationTests/testOnly *PostgreSQLIntegrationSpec"

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ All PostgreSQL tests passed!"
    echo ""
    echo "📚 What was tested:"
    echo "   - Traditional schema with columns"
    echo "   - Firebase model with JSONB storage"
    echo "   - Loading DDEX ERN XML (out.xml)"
    echo "   - Loading multiple XML files"
    echo "   - Querying JSONB data"
    echo ""
    echo "🔍 Key features:"
    echo "   - Automatic table creation"
    echo "   - Schema inference from XML"
    echo "   - JSONB for flexible schema"
    echo "   - Batch inserts for performance"
    echo ""
else
    echo ""
    echo "❌ Tests failed"
    exit 1
fi
