# Integration Tests

This document describes the integration testing setup for slatr.

## Overview

slatr uses a separate `integration-tests` module to run real integration tests against emulated services using [Testcontainers](https://www.testcontainers.org/). This approach provides:

1. **True Integration Testing**: Tests run against actual service implementations, not mocks
2. **Isolation**: Integration tests are in a separate module from unit tests
3. **Reproducibility**: Docker containers ensure consistent test environments
4. **CI/CD Ready**: Tests can run in any environment with Docker support

## Project Structure

```
slatr/
├── modules/
│   ├── core/                    # Core library with unit tests
│   ├── cli/                     # CLI application
│   └── integration-tests/       # Integration tests module
│       └── src/test/scala/
│           └── io/slatr/
│               └── converter/
│                   └── BigQueryIntegrationSpec.scala
├── build.sbt                    # Includes integrationTests module
└── project/Dependencies.scala    # Test dependencies
```

## Dependencies

Integration tests use these additional dependencies:

```scala
val testcontainers = "org.testcontainers" % "testcontainers" % "1.19.3" % "it,test"
val testcontainersScalatest = "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.0" % "it,test"
```

## BigQuery Integration Tests

### Prerequisites

- **Docker**: Must be installed and running
- **Docker Image**: `ghcr.io/goccy/bigquery-emulator:latest`
- **Internet**: Required to pull Docker images (first run only)

### What's Tested

The `BigQueryIntegrationSpec` tests the following scenarios:

1. **Schema Creation & Data Insertion**
   - Creates tables with inferred schema
   - Inserts data with various types (INT64, STRING, FLOAT64, DATE)
   - Verifies schema correctness
   - Queries and validates inserted data

2. **Write Modes**
   - **Append Mode**: Adds data to existing tables
   - **Overwrite Mode**: Deletes existing data before insert
   - **ErrorIfExists Mode**: Fails if table already exists

3. **Data Types**
   - Integers (INT64)
   - Strings (STRING)
   - Doubles (FLOAT64)
   - Booleans (BOOL)
   - Dates (DATE)
   - Arrays (REPEATED fields)

4. **Array Handling**
   - REPEATED mode for array fields
   - Correct insertion and retrieval of array data

### Running the Tests

```bash
# Run all integration tests
sbt integrationTests/test

# Run specific test
sbt "integrationTests/testOnly io.slatr.converter.BigQueryIntegrationSpec"

# Run with verbose output
sbt "integrationTests/testOnly io.slatr.converter.BigQueryIntegrationSpec" -- -oF
```

### Test Execution Flow

1. **Container Startup**: Testcontainers starts the BigQuery emulator
   - Listens on dynamically allocated ports (HTTP: 9050, gRPC: 9060)
   - Pre-creates project `test-project` and dataset `test_dataset`
   - Waits for "gRPC server listening" log message

2. **Test Execution**: Each test:
   - Creates a BigQuery client pointing to the emulator
   - Defines a schema using slatr's data model
   - Creates a `BigQueryWriter` with a factory function
   - Writes test data
   - Verifies results via BigQuery API queries
   - Cleans up test tables

3. **Container Shutdown**: Testcontainers automatically stops and removes the container

### Test Output Example

```
[info] BigQueryIntegrationSpec:
[info] BigQueryWriter with emulator
[info] - should create table with inferred schema and insert data
[info] - should handle append mode correctly
[info] - should handle overwrite mode correctly
[info] - should handle arrays correctly
[info] Run completed in 23 seconds, 451 milliseconds.
[info] Total number of tests run: 4
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 4, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

## Architectural Improvements

### Refactored BigQueryWriter

The `BigQueryWriter` was refactored to support integration testing:

**Before:**
```scala
class BigQueryWriter(xmlParser: XmlStreamParser) {
  def write(xmlFile: File, schema: Schema, config: BigQueryConfig): Try[TableId]
}
```

**After:**
```scala
class BigQueryWriter(
  schema: Schema,
  config: BigQueryConfig,
  bigQueryFactory: Option[() => BigQuery] = None  // ← Dependency injection for testing
) {
  def write(rows: Iterator[Map[String, Any]]): TableId  // ← Direct data writing
  def writeFromXml(xmlFile: File, xmlParser: XmlStreamParser): Try[TableId]  // ← Backwards compat
}
```

**Benefits:**
- **Testability**: Factory function allows injecting mock/emulator clients
- **Separation of Concerns**: Writing data is separate from parsing XML
- **Flexibility**: Can write data from any source, not just XML files
- **Backwards Compatibility**: Existing CLI code continues to work

## Troubleshooting

### Docker Issues

**Problem**: Tests fail with "Could not find a valid Docker environment"

**Solution**:
```bash
# Check Docker is running
docker ps

# On macOS, ensure Docker Desktop is running
open -a Docker
```

### Port Conflicts

**Problem**: Tests fail with port binding errors

**Solution**: Testcontainers uses dynamic port allocation, so conflicts are rare. If they occur:
```bash
# Check what's using the ports
lsof -i :9050
lsof -i :9060

# Kill conflicting processes or restart Docker
```

### Image Pull Issues

**Problem**: "Unable to pull image: ghcr.io/goccy/bigquery-emulator:latest"

**Solution**:
```bash
# Manually pull the image
docker pull ghcr.io/goccy/bigquery-emulator:latest

# Verify image exists
docker images | grep bigquery-emulator
```

### Slow Test Execution

**Problem**: Tests take a long time

**Explanation**: First run is slow due to:
1. Docker image download (~200MB)
2. Container startup time (~5-10 seconds)

**Mitigation**:
- Image is cached after first pull
- Run tests in parallel where possible
- Use `testOnly` to run specific tests during development

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up JDK 17
        uses: actions/setup-java@v3
        with:
          java-version: '17'
          distribution: 'temurin'
      
      - name: Run integration tests
        run: sbt integrationTests/test
```

### Docker-in-Docker

If running in a containerized CI environment, ensure Docker-in-Docker is configured:

```yaml
services:
  docker:
    image: docker:dind
    privileged: true
```

## Future Enhancements

Potential additions to the integration test suite:

1. **Parquet Integration Tests**
   - Use filesystem-based validation
   - Verify file structure and schema
   - Test compression options

2. **End-to-End Workflow Tests**
   - XML → Schema Inference → BigQuery
   - XML → Parquet → Spark validation
   - Multiple output formats from same source

3. **Performance Tests**
   - Large file handling
   - Chunking strategy validation
   - Memory usage profiling

4. **Error Handling Tests**
   - Network failures
   - Invalid schemas
   - Malformed data

## References

- [Testcontainers Documentation](https://www.testcontainers.org/)
- [BigQuery Emulator](https://github.com/goccy/bigquery-emulator)
- [testcontainers-scala](https://github.com/testcontainers/testcontainers-scala)
- [ScalaTest Documentation](https://www.scalatest.org/)
