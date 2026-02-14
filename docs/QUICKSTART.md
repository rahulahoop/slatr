# Slatr Quick Start Guide

## What is Slatr?

Slatr is a powerful tool for converting XML files to modern data formats and loading them into BigQuery with automatic schema inference.

## Installation

```bash
# Build the JAR
just build

# Or use sbt directly
sbt cli/assembly

# The JAR will be at: modules/cli/target/scala-2.13/slatr.jar
```

## Quick Examples

### 1. Convert XML to JSON

```bash
# Simple conversion
slatr convert input.xml -o output.json

# With pretty printing
slatr convert input.xml -o output.json --pretty

# Convert to JSON Lines
slatr convert input.xml -o output.jsonl --format jsonl
```

### 2. Load XML into BigQuery (Traditional Schema)

```bash
slatr to-bigquery input.xml \
  --project my-project \
  --dataset my-dataset \
  --table my-table
```

### 3. Load XML into BigQuery (Firebase Model)

```bash
# Recommended for complex/variable schemas
slatr to-bigquery input.xml \
  --project my-project \
  --dataset my-dataset \
  --table my-table \
  --firebase-model
```

## Try the DDEX Example

We've included a complete example using DDEX ERN (music industry) XML files:

```bash
# Run the automated example with BigQuery emulator
./scripts/load-ddex-to-bigquery-local.sh
```

This will:
- Start a local BigQuery emulator
- Load DDEX XML files using Firebase model
- Run queries to extract music metadata
- Show schema evolution capabilities

See [DDEX BigQuery Example](examples/ddex-bigquery-example.md) for details.

## Firebase Model Benefits

The Firebase model stores data as key-value pairs instead of columns:

**When to use:**
- XML has 100+ different fields
- Schema changes frequently
- Merging XMLs with different structures
- Avoiding BigQuery's 10,000 column limit

**Schema:**
```sql
CREATE TABLE dataset.table (
  fields ARRAY<STRUCT<
    name STRING,
    value STRING
  >>
)
```

**Querying:**
```sql
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'id') as id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'name') as name
FROM `project.dataset.table`
```

## Common Tasks

### Preview Schema Without Converting
```bash
slatr convert input.xml -o output.json --dry-run
```

### Validate XML Against XSD
```bash
slatr convert input.xml -o output.json --validate
```

### Test with BigQuery Emulator
```bash
# Start emulator
just start-emulator

# Load data (in another terminal)
slatr to-bigquery input.xml \
  -p test-project \
  -d test_dataset \
  -t test_table

# Stop emulator
just stop-emulator
```

## Available Commands

```bash
# Show all available just recipes
just --list

# Run tests
just test

# Run integration tests
just it

# Build JAR
just build

# Format code
just fmt

# Compile
just compile
```

## Documentation

- [Firebase Model Guide](features/firebase-model.md) - Detailed Firebase model documentation
- [DDEX Example](examples/ddex-bigquery-example.md) - Complete DDEX ERN example
- [XML to JSON Conversion](features/xml-to-json-conversion.md) - CLI conversion guide

## Architecture

```
XML Files
    ↓
Schema Inference (automatic)
    ↓
Conversion (JSON/JSONL/Parquet/BigQuery)
    ↓
Output (files or BigQuery table)
```

## Support

- Report issues: GitHub Issues
- Check examples: `examples/` directory
- Run tests: `just test` or `just it`

## Next Steps

1. Try converting your own XML files
2. Run the DDEX example to see Firebase model in action
3. Set up BigQuery integration for your project
4. Create custom schemas and configurations
