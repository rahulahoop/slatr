# Testing Guide

## Running Tests

### Unit Tests
```bash
# Run all unit tests
just test

# Or with sbt
sbt test
```

### Integration Tests

We use PostgreSQL for integration testing instead of BigQuery emulator because:
- BigQuery emulator doesn't support ARM64 (Apple Silicon)
- PostgreSQL is more reliable and mature
- Testcontainers handles everything automatically

```bash
# Run all integration tests
just it

# Or with sbt
sbt integrationTests/test

# Run specific test
sbt "integrationTests/testOnly *PostgreSQLIntegrationSpec"
```

### Quick Integration Test
```bash
# Automated PostgreSQL test with DDEX files
./scripts/test-postgresql-local.sh
```

## Test Requirements

- **Docker** must be running (for Testcontainers)
- **Java** 11 or higher
- **sbt** 1.9+
- **Apple Silicon (ARM64)**: BigQuery emulator uses AMD64 emulation via Rosetta 2

## What Gets Tested

### PostgreSQL Integration Tests
- Traditional schema with typed columns
- Firebase model with JSONB storage
- DDEX ERN XML file loading
- Multiple file loading and schema evolution
- JSONB querying

### Test Data
- `examples/simple.xml` - Simple book catalog
- `examples/nested.xml` - Nested employee data
- `out.xml` - DDEX ERN 3.8.2 release notification
- `bout.xml` - Additional DDEX message

## Troubleshooting

### Docker Not Running
```
Error: Docker is not running
```
**Solution:** Start Docker Desktop

### Container Port Conflicts
```
Error: Port 5432 already in use
```
**Solution:** Stop local PostgreSQL or change the test to use a different port

### Test Timeouts
```
Error: Container failed to start
```
**Solution:** Increase Docker resources (Memory/CPU in Docker Desktop settings)

## CI/CD

Integration tests use Testcontainers which:
- Automatically pulls PostgreSQL Docker image
- Starts fresh container for each test suite
- Cleans up after tests complete
- Works in CI environments (GitHub Actions, etc.)

## BigQuery Emulator vs PostgreSQL

### BigQuery Emulator
The BigQuery emulator works via AMD64 emulation on ARM64 (Apple Silicon):
- ⚠️ Requires Rosetta 2 translation (slower)
- ⚠️ Experimental and limited SQL compatibility
- ✅ Tests actual BigQuery writer code
- ✅ Tests Firebase model schema

We use `--platform linux/amd64` flag to enable emulation.

### PostgreSQL
PostgreSQL tests are recommended for development:
- ✅ Native ARM64 support (faster)
- ✅ Mature and reliable
- ✅ Better JSONB support than BigQuery JSON
- ✅ Full SQL compatibility
- ✅ Production-ready

**Recommendation**: Use PostgreSQL for daily development, BigQuery emulator for pre-production validation.

## Writing New Tests

See `PostgreSQLIntegrationSpec.scala` for examples:

```scala
class MyIntegrationSpec extends AnyFlatSpec 
  with Matchers 
  with ForAllTestContainer {
  
  override val container: PostgreSQLContainer = 
    PostgreSQLContainer("postgres:16-alpine")
  
  "MyTest" should "do something" in {
    // Use container.jdbcUrl, container.username, etc.
    val config = PostgreSQLConfig(
      host = container.host,
      port = container.mappedPort(5432),
      database = container.databaseName,
      // ... other config
    )
    
    // Your test code
  }
}
```
