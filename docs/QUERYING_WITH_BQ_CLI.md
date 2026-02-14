# Querying BigQuery Emulator with bq CLI

This guide shows how to use the `bq` command-line tool to query the BigQuery emulator.

## Installation

### Option 1: Install Google Cloud SDK (includes bq)

```bash
# macOS
brew install --cask google-cloud-sdk

# Or download from: https://cloud.google.com/sdk/docs/install
```

### Option 2: Install bq standalone (Python)

```bash
pip install google-cloud-bigquery
```

## Configuration for Emulator

The `bq` CLI doesn't natively support custom endpoints like `http://localhost:9050`. However, you have several alternatives:

### Alternative 1: Use curl (No Installation Needed) ✅ RECOMMENDED

This is the simplest approach and works immediately:

```bash
# Run the provided script
./scripts/query-bigquery-curl.sh

# Or query directly
curl -X POST "http://localhost:9050/bigquery/v2/projects/test-project/queries" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT COUNT(*) as count FROM `test-project.music_metadata.release_notifications`",
    "useLegacySql": false
  }' | jq '.'
```

### Alternative 2: Use Python Client Library

Create a simple Python script:

```python
from google.cloud import bigquery
from google.cloud import NoCredentials

# Connect to emulator
client = bigquery.Client(
    project="test-project",
    client_options={"api_endpoint": "http://localhost:9050"},
    credentials=NoCredentials.getInstance()
)

# Run query
query = """
    SELECT COUNT(*) as count 
    FROM `test-project.music_metadata.release_notifications`
"""
results = client.query(query).result()

for row in results:
    print(f"Count: {row['count']}")
```

Run it:
```bash
python3 scripts/query-bigquery-emulator.py
```

### Alternative 3: Modify bq CLI to Use Emulator (Advanced)

The `bq` CLI doesn't support the `--api_endpoint` flag for the emulator. You'd need to:

1. Set up a proxy that redirects BigQuery API calls to localhost:9050
2. Or modify the `bq` source code
3. Or use environment variables (doesn't work reliably)

**This is complex and not recommended.** Use curl or Python instead.

## Example Queries

Once the emulator is running (`./scripts/load-ddex-to-bigquery-local.sh skip-tests`):

### Using curl

```bash
# Count rows
curl -s -X POST "http://localhost:9050/bigquery/v2/projects/test-project/queries" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT COUNT(*) as count FROM `test-project.music_metadata.release_notifications`",
    "useLegacySql": false
  }' | jq -r '.rows[].f[].v'

# List tables
curl -s "http://localhost:9050/bigquery/v2/projects/test-project/datasets/music_metadata/tables" | jq '.'

# Get table schema
curl -s "http://localhost:9050/bigquery/v2/projects/test-project/datasets/music_metadata/tables/release_notifications" | jq '.schema'
```

### Using Python

```python
from google.cloud import bigquery
from google.cloud import NoCredentials

client = bigquery.Client(
    project="test-project",
    client_options={"api_endpoint": "http://localhost:9050"},
    credentials=NoCredentials.getInstance()
)

# List datasets
for dataset in client.list_datasets():
    print(dataset.dataset_id)

# List tables
for table in client.list_tables("music_metadata"):
    print(f"{table.table_id}: {table.num_rows} rows")

# Run query
query = "SELECT * FROM `test-project.music_metadata.release_notifications` LIMIT 5"
for row in client.query(query).result():
    print(row)
```

## Why Not bq CLI?

The official `bq` CLI tool has limitations:

1. **No custom endpoint support** - Can't point to `http://localhost:9050`
2. **Requires authentication** - Even with `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE`
3. **Hardcoded to `bigquery.googleapis.com`** - Can't override the API endpoint

The BigQuery emulator documentation recommends using:
- REST API (curl)
- Client libraries (Python, Java, Go, etc.)
- Direct gRPC calls

## Recommended Approach

**Use the provided scripts:**

```bash
# Start emulator (keeps running)
./scripts/load-ddex-to-bigquery-local.sh skip-tests

# Query with curl (simple)
./scripts/query-bigquery-curl.sh

# Query with Python (interactive)
python3 scripts/query-bigquery-emulator.py
```

These scripts are specifically designed for the emulator and work immediately without complex setup.

## Production BigQuery

When using real BigQuery (not the emulator), the `bq` CLI works normally:

```bash
# Authenticate
gcloud auth login

# Set project
gcloud config set project YOUR_PROJECT_ID

# Run queries
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `dataset.table`'

# Load data
bq load --source_format=NEWLINE_DELIMITED_JSON \
  dataset.table \
  data.jsonl \
  schema.json
```

## Summary

| Tool | Emulator Support | Ease of Use | Recommended |
|------|------------------|-------------|-------------|
| curl | ✅ Yes | ⭐⭐⭐⭐⭐ Easy | ✅ Yes |
| Python Client | ✅ Yes | ⭐⭐⭐⭐ Easy | ✅ Yes |
| bq CLI | ❌ No | ⭐ Hard | ❌ No |
| gRPC | ✅ Yes | ⭐⭐ Medium | 🤷 Advanced |

**For the emulator, use curl or Python. For production, use bq CLI.**
