# Querying BigQuery Emulator

This guide shows how to query the BigQuery emulator after loading your DDEX data.

## Quick Start

### 1. Start Emulator (Without Tests)

```bash
# Start emulator and keep it running for queries
./scripts/load-ddex-to-bigquery-local.sh skip-tests
```

This will:
- Start BigQuery emulator on ports 9050 (REST) and 9060 (gRPC)
- Keep running until you stop it
- Make emulator available for queries

### 2. Query the Data

You have three options:

## Option 1: curl (No Dependencies)

```bash
# Run example queries
./scripts/query-bigquery-curl.sh
```

Or manually:

```bash
curl -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT COUNT(*) as count FROM `test-project.music_metadata.release_notifications`",
    "useLegacySql": false
  }' | jq
```

## Option 2: Python Client

```bash
# Install client library
pip install google-cloud-bigquery

# Run interactive query tool
python3 scripts/query-bigquery-emulator.py
```

Features:
- Lists all tables
- Shows field names
- Extracts music metadata
- Interactive SQL mode

## Option 3: bq CLI

```bash
# Configure bq to use emulator
export BIGQUERY_ENDPOINT=http://localhost:9050

# Run queries
bq query --nouse_legacy_sql \
  'SELECT COUNT(*) FROM `test-project.music_metadata.release_notifications`'
```

## Example Queries

### Count Rows

```sql
SELECT COUNT(*) as count 
FROM `test-project.music_metadata.release_notifications`
```

### Show All Field Names

```sql
SELECT DISTINCT field.name as field_name
FROM `test-project.music_metadata.release_notifications`,
UNNEST(fields) AS field
ORDER BY field_name
```

### Extract Music Metadata

```sql
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'MessageId') as message_id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
  (SELECT value FROM UNNEST(fields) WHERE name LIKE '%TitleText%' LIMIT 1) as title,
  (SELECT value FROM UNNEST(fields) WHERE name = 'DisplayArtistName') as artist,
  (SELECT value FROM UNNEST(fields) WHERE name = 'GenreText') as genre
FROM `test-project.music_metadata.release_notifications`
LIMIT 10
```

### Find Specific ISRC

```sql
SELECT *
FROM `test-project.music_metadata.release_notifications`
WHERE EXISTS (
  SELECT 1 FROM UNNEST(fields) 
  WHERE name = 'ISRC' AND value = 'USAT12345678'
)
```

### Search by Artist

```sql
SELECT *
FROM `test-project.music_metadata.release_notifications`
WHERE EXISTS (
  SELECT 1 FROM UNNEST(fields) 
  WHERE name = 'DisplayArtistName' AND value LIKE '%Example%'
)
```

## Workflow

### Full Workflow with Queries

```bash
# 1. Start emulator without tests
./scripts/load-ddex-to-bigquery-local.sh skip-tests

# 2. In another terminal, run tests to load data
DOCKER_DEFAULT_PLATFORM=linux/amd64 sbt "integrationTests/testOnly *DdexToBigQuerySpec"

# 3. Query the data
./scripts/query-bigquery-curl.sh

# Or use Python
python3 scripts/query-bigquery-emulator.py

# 4. Stop when done
docker stop bigquery-emulator-ddex
```

### Development Workflow

```bash
# Terminal 1: Start emulator (leave running)
just start-emulator

# Terminal 2: Run tests whenever needed
sbt "integrationTests/testOnly *DdexToBigQuerySpec"

# Terminal 2: Query data
./scripts/query-bigquery-curl.sh

# Terminal 1: Stop emulator (Ctrl+C or)
docker stop bigquery-emulator
```

## Debugging

### Check if Emulator is Running

```bash
docker ps | grep bigquery-emulator

# Should show:
# CONTAINER ID   IMAGE                                    PORTS
# xxxxxxxxxxxx   ghcr.io/goccy/bigquery-emulator:latest  0.0.0.0:9050->9050/tcp, ...
```

### View Emulator Logs

```bash
docker logs -f bigquery-emulator-ddex
```

### Test Connection

```bash
curl http://localhost:9050
# Should return some HTML or JSON response
```

### Port Already in Use

```bash
# Stop any conflicting containers
docker stop $(docker ps -q --filter "publish=9050")

# Or change ports in the script
-p 9051:9050 -p 9061:9060
```

## REST API Endpoints

The emulator exposes BigQuery REST API v2:

```bash
# List datasets
curl http://localhost:9050/bigquery/v2/projects/test-project/datasets

# List tables
curl http://localhost:9050/bigquery/v2/projects/test-project/datasets/music_metadata/tables

# Run query
curl -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT 1", "useLegacySql": false}'

# Get table info
curl http://localhost:9050/bigquery/v2/projects/test-project/datasets/music_metadata/tables/release_notifications
```

## Limitations

The BigQuery emulator has some limitations:

- ❌ Not all BigQuery SQL functions supported
- ❌ No authentication/authorization
- ❌ Limited to local development only
- ❌ Some complex queries may fail
- ✅ Good for basic testing and development

For full BigQuery compatibility, test against real BigQuery.

## Tips

1. **Use jq for JSON formatting**: `| jq` at the end of curl commands
2. **Save queries to files**: Create `.sql` files and reference them
3. **Use views**: Create views for frequently-used queries
4. **Monitor logs**: Watch `docker logs` for errors
5. **Restart if stuck**: Sometimes a fresh emulator helps

## Next Steps

- Try the example queries above
- Modify queries for your data structure
- Integrate with your application
- Test before deploying to production BigQuery
