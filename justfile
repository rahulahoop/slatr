# Justfile for slatr project

# Default recipe - show help
default:
	@just --list

# Run unit tests
test:
	sbt test

# Run integration tests
it:
	sbt integrationTests/test

# Build the JAR file
build:
	sbt cli/assembly

# Run all tests and build JAR
all: test it build

# Clean build artifacts
clean:
	sbt clean

# Format code with scalafmt
fmt:
	sbt scalafmtAll

# Compile the project
compile:
	sbt compile

# Run tests with coverage
test-coverage:
	sbt clean coverage test coverageReport

# Run integration tests with verbose output
it-verbose:
	sbt "integrationTests/testOnly * -- -oF"

# Build and run CLI
run:
	sbt cli/run

# Build and run convert command help
run-convert:
	sbt "cli/run convert --help"

# Build and run to-bigquery command help
run-to-bigquery:
	sbt "cli/run to-bigquery --help"

# Pull BigQuery emulator image (AMD64 for ARM64 compatibility)
pull-emulator:
	docker pull --platform linux/amd64 ghcr.io/goccy/bigquery-emulator:latest

# Start BigQuery emulator standalone
start-emulator:
	docker run -it --rm --platform linux/amd64 -p 9050:9050 -p 9060:9060 ghcr.io/goccy/bigquery-emulator:latest --project=test-project --dataset=test_dataset

# Stop BigQuery emulator
stop-emulator:
	@docker ps | grep bigquery-emulator | awk '{print $$1}' | xargs docker stop || echo "No emulator running"

# Check Docker status
docker-status:
	@docker ps
	@docker images | grep bigquery-emulator || echo "No bigquery-emulator images"

# Start emulator in background for development
dev:
	@echo "Starting BigQuery emulator (AMD64 emulation for ARM64)..."
	docker run -d --rm --platform linux/amd64 -p 9050:9050 -p 9060:9060 --name bigquery-emulator ghcr.io/goccy/bigquery-emulator:latest --project=test-project --dataset=test_dataset
	@sleep 3
	@echo "BigQuery emulator running on:"
	@echo "  REST API: http://localhost:9050"
	@echo "  gRPC API: localhost:9060"
	@echo ""
	@echo "To stop: docker stop bigquery-emulator"

# Check emulator status
check-emulator:
	@docker ps | grep bigquery-emulator || echo "Emulator not running"

# Test emulator connection
test-emulator:
	@sleep 5 && curl -s http://localhost:9050 | head -20

# Build and run integration tests against local emulator  
it-local:
	@echo "Starting emulator (AMD64 emulation)..."
	docker run -d --rm --platform linux/amd64 -p 9050:9050 -p 9060:9060 --name bigquery-emulator ghcr.io/goccy/bigquery-emulator:latest --project=test-project --dataset=test_dataset
	@sleep 5
	@echo "Running integration tests..."
	DOCKER_DEFAULT_PLATFORM=linux/amd64 sbt integrationTests/test
	@echo "Stopping emulator..."
	docker stop bigquery-emulator

# Create a distributable package
package:
	@echo "Building JAR..."
	sbt cli/assembly
	@echo "Creating package directory..."
	mkdir -p slatr-package/examples
	cp modules/cli/target/scala-2.13/slatr.jar slatr-package/
	@if [ -f README.md ]; then cp README.md slatr-package/; fi
	@if [ -d docs ]; then cp -r docs slatr-package/; fi
	@if [ -d examples ]; then cp examples/*.xml slatr-package/examples/ 2>/dev/null || true; fi
	@echo "Package created in: slatr-package/"

# Build Docker image
docker-build:
	sbt cli/assembly
	docker build -t slatr:latest .

# Run slatr in Docker
docker-run:
	docker run -it --rm -v "{{justfile_directory()}}/examples:/examples" slatr:latest convert /examples/simple.xml -o /examples/simple.json

# Clean up Docker resources
docker-clean:
	@docker ps -a | grep bigquery-emulator | awk '{print $$1}' | xargs docker stop 2>/dev/null || true
	@docker ps -a | grep bigquery-emulator | awk '{print $$1}' | xargs docker rm 2>/dev/null || true
	@docker images | grep bigquery-emulator | awk '{print $$3}' | xargs docker rmi 2>/dev/null || true

# Show current status
status:
	@echo "=== slatr Project Status ==="
	@echo ""
	@echo "Build artifacts:"
	@ls -lh modules/cli/target/scala-2.13/*.jar 2>/dev/null || echo "  No JAR built yet"
	@echo ""
	@echo "Docker status:"
	@docker ps -a | head -10
	@echo ""
	@echo "Git status:"
	@git status --short

# Check for common issues
check:
	@echo "=== Checking for common issues ==="
	@docker ps > /dev/null 2>&1 && echo "✓ Docker is running" || echo "✗ Docker is not running"
	@docker images ghcr.io/goccy/bigquery-emulator:latest > /dev/null 2>&1 && echo "✓ BigQuery emulator image available" || echo "✗ BigQuery emulator image not available. Run: just pull-emulator"
	@[ -f build.sbt ] && echo "✓ build.sbt exists" || echo "✗ build.sbt missing"
	@[ -f justfile ] && echo "✓ justfile exists" || echo "✗ justfile missing"
	@echo ""
	@echo "Run 'just status' for more detailed project status"

# ── PostgreSQL playground ──────────────────────────────────────

# Start PostgreSQL playground (seeded with sample data)
pg-start:
	@echo "Starting PostgreSQL playground..."
	docker compose up -d postgres
	@echo ""
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 3
	@docker exec slatr-postgres pg_isready -U slatr -d music_metadata > /dev/null 2>&1 || sleep 2
	@docker exec slatr-postgres pg_isready -U slatr -d music_metadata > /dev/null 2>&1 || sleep 2
	@docker exec slatr-postgres pg_isready -U slatr -d music_metadata > /dev/null 2>&1 || sleep 2
	@docker exec slatr-postgres pg_isready -U slatr -d music_metadata > /dev/null 2>&1 || (echo "Error: PostgreSQL not ready after 9s" && exit 1)
	@echo "PostgreSQL is ready."
	@echo ""
	@echo "Connection details:"
	@echo "  Host:     localhost"
	@echo "  Port:     5432"
	@echo "  Database: music_metadata"
	@echo "  Username: slatr"
	@echo "  Password: slatr"
	@echo "  JDBC URL: jdbc:postgresql://localhost:5432/music_metadata"
	@echo ""
	@echo "Quick commands:"
	@echo "  just pg-psql                      # open psql shell"
	@echo "  just pg-load examples/out.xml      # load a DDEX XML"
	@echo "  just pg-query 'SELECT * FROM ddex_releases_flat'"
	@echo ""
	@echo "DBeaver: see docs/POSTGRES_PLAYGROUND.md"

# Stop PostgreSQL playground
pg-stop:
	docker compose down
	@echo "PostgreSQL stopped."

# Reset PostgreSQL (destroy data volume and recreate)
pg-reset:
	docker compose down -v
	@echo "Volume removed. Run 'just pg-start' to recreate."

# Open a psql shell in the playground
pg-psql:
	docker exec -it slatr-postgres psql -U slatr -d music_metadata

# Run a SQL query against the playground
pg-query sql:
	docker exec slatr-postgres psql -U slatr -d music_metadata -c "{{sql}}"

# Load a single XML file (firebase model by default)
pg-load file="examples/out.xml" mode="firebase":
	./scripts/load-ddex-to-postgres-local.sh --{{mode}} "{{file}}"

# Load ALL examples/*.xml (firebase: single ddex_releases table; traditional: one table per file)
pg-load-all mode="firebase":
	./scripts/load-ddex-to-postgres-local.sh --all --{{mode}}

# Show tables and row counts in the playground
pg-status:
	@docker exec slatr-postgres psql -U slatr -d music_metadata -c "\dt public.*"
	@echo ""
	@docker exec slatr-postgres psql -U slatr -d music_metadata -c "SELECT schemaname||'.'||relname AS table_name, n_live_tup AS row_count FROM pg_stat_user_tables ORDER BY relname;"

# Run PostgreSQL integration tests (uses Testcontainers, not the playground)
pg-test:
	sbt "integrationTests/testOnly *PostgreSQLIntegrationSpec"

# ── Documentation ─────────────────────────────────────────────

# Generate API documentation
docs:
	sbt doc
	@echo "Documentation generated in: modules/*/target/scala-2.13/api/"
