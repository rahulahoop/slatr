# Firebase Model Schema Explained

## The Schema Structure

The Firebase model uses a **single column** that contains an **array of key-value pairs**:

```
Table: sample_releases
├── fields (ARRAY<STRUCT>) REPEATED
    ├── name (STRING) NULLABLE
    └── value (STRING) NULLABLE
```

### In BigQuery SQL DDL:
```sql
CREATE TABLE sample_releases (
  fields ARRAY<STRUCT<
    name STRING,
    value STRING
  >>
)
```

### In JSON (as returned by the API):
```json
{
  "fields": [
    {
      "name": "fields",
      "type": "RECORD",
      "mode": "REPEATED",
      "fields": [
        {
          "name": "name",
          "type": "STRING",
          "mode": "NULLABLE"
        },
        {
          "name": "value",
          "type": "STRING",
          "mode": "NULLABLE"
        }
      ]
    }
  ]
}
```

## Traditional Schema vs Firebase Model

### Traditional Schema (Column-based)

For a music release with 5 fields, you'd have **5 columns**:

```sql
CREATE TABLE releases_traditional (
  MessageId STRING,
  ISRC STRING,
  Title STRING,
  Artist STRING,
  Genre STRING
)
```

**Schema:**
```
releases_traditional
├── MessageId (STRING)
├── ISRC (STRING)
├── Title (STRING)
├── Artist (STRING)
└── Genre (STRING)
```

**Problems:**
- ❌ BigQuery has a **10,000 column limit**
- ❌ DDEX ERN files have **500+ fields** 
- ❌ Adding new fields requires schema migration
- ❌ Different DDEX versions = different columns
- ❌ Schema evolution is complex

**Sample Row:**
```
MessageId  | ISRC          | Title         | Artist      | Genre
-----------|---------------|---------------|-------------|-------
MSG-001    | USAT20001234  | Sample Song 1 | Test Artist | Rock
```

---

### Firebase Model (Key-Value Array)

The same data with **1 column** containing key-value pairs:

```sql
CREATE TABLE releases_firebase (
  fields ARRAY<STRUCT<name STRING, value STRING>>
)
```

**Schema:**
```
releases_firebase
└── fields (ARRAY)
    └── STRUCT
        ├── name (STRING)   -- field name: "ISRC", "Title", etc.
        └── value (STRING)  -- field value: "USAT20001234", "Sample Song 1", etc.
```

**Advantages:**
- ✅ No column limit (store unlimited fields)
- ✅ Schema evolution is automatic
- ✅ Different DDEX versions = same table
- ✅ Add/remove fields without migration
- ✅ Flexible for variable schemas

**Sample Row (conceptual):**
```
fields: [
  {name: "MessageId", value: "MSG-001"},
  {name: "ISRC", value: "USAT20001234"},
  {name: "Title", value: "Sample Song 1"},
  {name: "Artist", value: "Test Artist"},
  {name: "Genre", value: "Rock"}
]
```

**Sample Row (actual JSON):**
```json
{
  "fields": [
    {"name": "MessageId", "value": "MSG-001"},
    {"name": "ISRC", "value": "USAT20001234"},
    {"name": "Title", "value": "Sample Song 1"},
    {"name": "Artist", "value": "Test Artist"},
    {"name": "Genre", "value": "Rock"}
  ]
}
```

## Visual Schema Comparison

### Traditional Schema (Multiple Columns)
```
┌─────────────────────────────────────────────────────────────────────┐
│ Table: releases_traditional                                         │
├──────────┬──────────────┬───────────────┬─────────────┬────────────┤
│ MessageId│ ISRC         │ Title         │ Artist      │ Genre      │
│ (STRING) │ (STRING)     │ (STRING)      │ (STRING)    │ (STRING)   │
├──────────┼──────────────┼───────────────┼─────────────┼────────────┤
│ MSG-001  │ USAT20001234 │ Sample Song 1 │ Test Artist │ Rock       │
│ MSG-002  │ USAT20005678 │ Sample Song 2 │ Another Art │ Jazz       │
└──────────┴──────────────┴───────────────┴─────────────┴────────────┘

Problems:
  - 5 fields = 5 columns
  - 500 fields = 500 columns ❌
  - 10,000 column limit ❌
```

### Firebase Model (Single Array Column)
```
┌─────────────────────────────────────────────────────────────────────┐
│ Table: releases_firebase                                            │
├─────────────────────────────────────────────────────────────────────┤
│ fields: ARRAY<STRUCT<name STRING, value STRING>>                   │
├─────────────────────────────────────────────────────────────────────┤
│ [{name: "MessageId", value: "MSG-001"},                            │
│  {name: "ISRC", value: "USAT20001234"},                            │
│  {name: "Title", value: "Sample Song 1"},                          │
│  {name: "Artist", value: "Test Artist"},                           │
│  {name: "Genre", value: "Rock"}]                                   │
├─────────────────────────────────────────────────────────────────────┤
│ [{name: "MessageId", value: "MSG-002"},                            │
│  {name: "ISRC", value: "USAT20005678"},                            │
│  {name: "Title", value: "Sample Song 2"},                          │
│  {name: "Artist", value: "Another Artist"},                        │
│  {name: "Genre", value: "Jazz"}]                                   │
└─────────────────────────────────────────────────────────────────────┘

Advantages:
  - 5 fields = 1 column ✅
  - 500 fields = 1 column ✅
  - Unlimited fields ✅
```

## Real Data Example

Here's actual data loaded in the emulator:

**Row 1:**
```
MessageId = MSG-001
ISRC = USAT20001234
Title = Sample Song 1
Artist = Test Artist
Genre = Rock
```

**Row 2:**
```
MessageId = MSG-002
ISRC = USAT20005678
Title = Sample Song 2
Artist = Another Artist
Genre = Jazz
```

**Row 3 (demonstrates schema evolution):**
```
MessageId = MSG-003
ISRC = USAT20009999
Title = Sample Song 3
Artist = Third Artist
Genre = Electronic
Year = 2024          ← New field added without schema migration!
```

Notice Row 3 has a **"Year" field** that Rows 1 and 2 don't have. This is **schema evolution** in action - no ALTER TABLE required!

## How to Enable Firebase Model

### Using slatr CLI

```bash
# Load XML to BigQuery with Firebase model
slatr to-bigquery \
  --input examples/out.xml \
  --project-id your-project \
  --dataset-id music_metadata \
  --table-id releases \
  --firebase-model \
  --write-mode overwrite
```

### Using Scala API

```scala
import io.slatr.model.BigQueryConfig
import io.slatr.converter.BigQueryWriter

val config = BigQueryConfig(
  projectId = "your-project",
  datasetId = "music_metadata",
  tableId = "releases",
  writeMode = WriteMode.Append,
  credentialsPath = None,
  useFirebaseModel = true  // ← Enable Firebase model
)

val writer = new BigQueryWriter(schema, config)
writer.writeFromXml(xmlFile, xmlParser, None)
```

### Configuration File

```hocon
bigquery {
  project-id = "your-project"
  dataset-id = "music_metadata"
  table-id = "releases"
  use-firebase-model = true  // ← Enable Firebase model
  write-mode = "append"
}
```

## Querying the Data

### Production BigQuery (UNNEST works)

Extract specific fields using UNNEST:

```sql
-- Extract specific fields
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'MessageId') as message_id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
  (SELECT value FROM UNNEST(fields) WHERE name = 'Title') as title,
  (SELECT value FROM UNNEST(fields) WHERE name = 'Artist') as artist
FROM `project.dataset.releases`
```

Find all distinct field names:

```sql
-- Discover all field names in the data
SELECT DISTINCT field.name as field_name
FROM `project.dataset.releases`,
UNNEST(fields) AS field
ORDER BY field_name
```

Create a view for easier querying:

```sql
-- Create a flattened view
CREATE VIEW releases_view AS
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'MessageId') as message_id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
  (SELECT value FROM UNNEST(fields) WHERE name = 'Title') as title,
  (SELECT value FROM UNNEST(fields) WHERE name = 'Artist') as artist,
  (SELECT value FROM UNNEST(fields) WHERE name = 'Genre') as genre,
  (SELECT value FROM UNNEST(fields) WHERE name = 'Year') as year
FROM `project.dataset.releases`
```

### BigQuery Emulator (UNNEST limitation)

**Note:** The BigQuery emulator has a known limitation with UNNEST queries on struct arrays. Use these queries instead:

```sql
-- Count total rows (works)
SELECT COUNT(*) as count 
FROM `test-project.music_metadata.sample_releases`

-- Get all data (works)
SELECT * 
FROM `test-project.music_metadata.sample_releases` 
LIMIT 10

-- UNNEST queries don't work in emulator (production only)
-- This will return 0 results in emulator:
SELECT (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc
FROM `test-project.music_metadata.sample_releases`
```

For the emulator, use curl or Python client to query the raw structure.

## When to Use Firebase Model

### ✅ Use Firebase Model When:

1. **Variable schema** - XML/JSON with many optional fields
2. **Large field count** - More than 100-200 fields
3. **Schema evolution** - Fields change between versions
4. **Multiple schemas** - Different document types in same table
5. **DDEX/XML ingestion** - Complex nested structures with 500+ fields

### ❌ Use Traditional Schema When:

1. **Fixed schema** - Always the same fields
2. **Small field count** - Less than 50 fields
3. **Performance critical** - Columnar queries on specific fields
4. **BI tools** - Direct connection to Tableau/Looker
5. **Simple data** - CSV-like structures

## Performance Considerations

### Storage

- **Traditional**: More efficient storage (columnar compression)
- **Firebase**: Slightly larger due to field names stored per row

### Query Performance

- **Traditional**: Faster for SELECT on specific columns
- **Firebase**: Requires UNNEST (slower but flexible)

### Recommendation

For DDEX and similar XML formats with 500+ fields, the Firebase model is **the only viable option** due to BigQuery's column limit. The query performance trade-off is acceptable for the flexibility gained.

## Testing

Run the integration tests to verify Firebase model:

```bash
# Start emulator and load sample data
./scripts/load-ddex-to-bigquery-local.sh skip-tests
./scripts/load-sample-data-to-emulator.sh

# Run integration tests
export DOCKER_DEFAULT_PLATFORM=linux/amd64
sbt "integrationTests/testOnly *DdexToBigQuerySpec"

# Query the data
./scripts/query-emulator.sh "SELECT COUNT(*) FROM \`test-project.music_metadata.sample_releases\`"
```

Expected results:
- ✅ All tests pass
- ✅ Data loaded successfully
- ✅ Schema structure correct
- ⚠️ UNNEST queries return 0 (emulator limitation)

## Summary

The Firebase model is a powerful pattern for storing variable-schema data in BigQuery:

| Feature | Traditional | Firebase Model |
|---------|-------------|----------------|
| Column Limit | 10,000 ❌ | Unlimited ✅ |
| Schema Evolution | Complex ❌ | Automatic ✅ |
| DDEX Support | No (500+ fields) ❌ | Yes ✅ |
| Query Performance | Fast ⚡ | Moderate 🚀 |
| BI Tool Support | Native ✅ | Via Views ✅ |
| Storage Efficiency | High ✅ | Good ✅ |

For DDEX ERN and similar XML formats, **Firebase model is the recommended approach**.
