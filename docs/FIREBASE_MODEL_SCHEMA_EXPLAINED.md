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

## Real Data from Emulator

Let me show you what the actual data looks like:

<function_calls>
<invoke name="mcp_bash">
<parameter name="command">curl -s -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM `test-project.music_metadata.sample_releases` LIMIT 1", "useLegacySql": false}' | jq -r '.rows[0].f[0].v[] | "\(.v.f[0].v) = \(.v.f[1].v)"'