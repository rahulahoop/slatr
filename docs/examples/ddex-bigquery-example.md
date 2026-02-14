# DDEX ERN to BigQuery (Production)

> **Note:** For local testing, use PostgreSQL instead. The BigQuery emulator doesn't support ARM64 (Apple Silicon) and is unreliable. See [DDEX PostgreSQL Example](ddex-postgresql-example.md).

## Production BigQuery Usage

When you're ready to use real BigQuery (not emulator):

### Prerequisites
- GCP project with BigQuery enabled
- Service account with BigQuery Data Editor role
- Service account JSON key file

### Load DDEX Files

```bash
# Using service account credentials
slatr to-bigquery out.xml \
  --project your-gcp-project \
  --dataset music_metadata \
  --table release_notifications \
  --credentials /path/to/service-account.json \
  --firebase-model

# Using Application Default Credentials
gcloud auth application-default login
slatr to-bigquery out.xml \
  --project your-gcp-project \
  --dataset music_metadata \
  --table release_notifications \
  --firebase-model
```

### Traditional vs Firebase Model

**Traditional Model** (columns):
```bash
# Creates separate column for each field
# Not recommended for DDEX - hits column limits!
slatr to-bigquery out.xml \
  --project your-project \
  --dataset music_metadata \
  --table releases_traditional
```

**Firebase Model** (recommended):
```bash
# Stores as array of key-value structs
# No column limits!
slatr to-bigquery out.xml \
  --project your-project \
  --dataset music_metadata \
  --table releases_firebase \
  --firebase-model
```

### Querying Firebase Model in BigQuery

```sql
-- Extract specific fields
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'MessageId') as message_id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
  (SELECT value FROM UNNEST(fields) WHERE name LIKE '%TitleText%' LIMIT 1) as title
FROM `your-project.music_metadata.releases_firebase`
```

### Create View for Easy Access

```sql
CREATE VIEW `your-project.music_metadata.releases_view` AS
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'MessageId') as message_id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
  (SELECT value FROM UNNEST(fields) WHERE name = 'GRid') as grid,
  (SELECT value FROM UNNEST(fields) WHERE name LIKE '%TitleText%' LIMIT 1) as title,
  (SELECT value FROM UNNEST(fields) WHERE name = 'DisplayArtistName') as artist,
  (SELECT value FROM UNNEST(fields) WHERE name = 'GenreText') as genre
FROM `your-project.music_metadata.releases_firebase`
```

## Batch Loading

For production data pipelines:

```bash
#!/bin/bash
# Load all DDEX files from a directory

for file in ddex_messages/*.xml; do
  echo "Loading $file..."
  slatr to-bigquery "$file" \
    --project your-project \
    --dataset music_metadata \
    --table release_notifications \
    --firebase-model \
    --write-mode append
done
```

## Cost Optimization

BigQuery charges for:
- Storage (~$0.02/GB/month)
- Queries (~$5/TB scanned)

Tips:
1. Use partitioning by date
2. Cluster by frequently-filtered fields
3. Use materialized views for common queries
4. Avoid `SELECT *` - specify columns

## Why Firebase Model for DDEX?

DDEX ERN messages can have 100+ fields. Benefits:

✅ **No Column Limits** - BigQuery has 10,000 column max
✅ **Schema Evolution** - New DDEX versions work automatically  
✅ **Easy Merging** - Combine different message types
✅ **Flexible Queries** - Extract any field dynamically

## Local Development

**Use PostgreSQL for local testing:**
- See [DDEX PostgreSQL Example](ddex-postgresql-example.md)
- Run `./scripts/test-postgresql-local.sh`
- Works on all platforms including Apple Silicon

## Monitoring

```sql
-- Check table size
SELECT 
  table_id,
  size_bytes / 1024 / 1024 as size_mb,
  row_count
FROM `your-project.music_metadata.__TABLES__`
WHERE table_id = 'release_notifications'

-- Find all field names
SELECT DISTINCT field.name, COUNT(*) as occurrences
FROM `your-project.music_metadata.releases_firebase`,
UNNEST(fields) AS field
GROUP BY field.name
ORDER BY occurrences DESC
```

## See Also

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Firebase Model Guide](../features/firebase-model.md)
- [PostgreSQL Alternative](ddex-postgresql-example.md) (for local testing)
