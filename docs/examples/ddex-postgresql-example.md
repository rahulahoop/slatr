## DDEX ERN to PostgreSQL Example

This example demonstrates loading DDEX ERN (Electronic Release Notification) XML files into PostgreSQL using the Firebase-style JSONB model.

## Why PostgreSQL?

PostgreSQL offers several advantages for XML data:
- **JSONB Support**: Native JSON storage with powerful querying
- **No Column Limits**: Unlike BigQuery's 10,000 columns
- **Flexible Indexing**: Create indexes on JSONB fields
- **Full-Text Search**: Built-in text search on JSON data
- **Mature Ecosystem**: Well-tested and widely deployed

## Firebase Model in PostgreSQL

Instead of creating hundreds of columns, we use a JSONB column:

```sql
CREATE TABLE ddex_releases (
  id SERIAL PRIMARY KEY,
  data JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

This allows storing complex DDEX messages regardless of field count.

## Running the Example

### Automated Test
```bash
# Run the complete test suite
./scripts/test-postgresql-local.sh
```

This uses Testcontainers to:
1. Automatically start PostgreSQL in Docker
2. Load DDEX XML files
3. Run queries
4. Clean up

### Manual Testing

If you have PostgreSQL running locally:

```bash
# Set up database
createdb music_metadata

# Load DDEX file (when CLI command is ready)
slatr to-postgresql out.xml \
  --host localhost \
  --database music_metadata \
  --table ddex_releases \
  --username your_user \
  --password your_password \
  --firebase-model
```

## Querying JSONB Data

### Extract Specific Fields

PostgreSQL's JSONB operators make queries powerful:

```sql
-- Extract values from JSONB array
SELECT 
  id,
  data #> '{0,value}' as message_id,
  created_at
FROM ddex_releases;

-- Filter by JSONB field
SELECT *
FROM ddex_releases
WHERE data @> '[{"name": "ISRC", "value": "USAT12345678"}]';

-- Full-text search in JSONB
SELECT *
FROM ddex_releases
WHERE data::text LIKE '%Example Artist%';
```

### Create Indexes for Performance

```sql
-- Index the entire JSONB column
CREATE INDEX idx_ddex_data ON ddex_releases USING GIN (data);

-- Index specific JSON path
CREATE INDEX idx_ddex_isrc ON ddex_releases 
USING btree ((data->0->>'value')) 
WHERE data->0->>'name' = 'ISRC';
```

### Create Materialized View

```sql
CREATE MATERIALIZED VIEW ddex_releases_view AS
SELECT 
  id,
  (SELECT value FROM jsonb_array_elements(data) 
   WHERE value->>'name' = 'MessageId' LIMIT 1) as message_id,
  (SELECT value FROM jsonb_array_elements(data) 
   WHERE value->>'name' = 'ISRC' LIMIT 1) as isrc,
  (SELECT value FROM jsonb_array_elements(data) 
   WHERE value->>'name' LIKE '%TitleText%' LIMIT 1) as title,
  (SELECT value FROM jsonb_array_elements(data) 
   WHERE value->>'name' = 'DisplayArtistName' LIMIT 1) as artist,
  created_at
FROM ddex_releases;

-- Refresh periodically
REFRESH MATERIALIZED VIEW ddex_releases_view;
```

## Integration Test Features

The test suite demonstrates:

### 1. Traditional Schema
```scala
// Creates columns for each field
val schema = Schema("root", Map(
  "id" -> Field("id", DataType.IntType),
  "name" -> Field("name", DataType.StringType),
  "price" -> Field("price", DataType.DoubleType)
))
```

Creates table:
```sql
CREATE TABLE products (
  id INTEGER NOT NULL,
  name TEXT NOT NULL,
  price DOUBLE PRECISION
)
```

### 2. Firebase Model
```scala
val config = PostgreSQLConfig(
  useFirebaseModel = true
  // ... other config
)
```

Creates table:
```sql
CREATE TABLE firebase_test (
  id SERIAL PRIMARY KEY,
  data JSONB NOT NULL
)
```

### 3. DDEX Loading
```scala
// Automatically infers schema and loads
val writer = PostgreSQLWriter(schema, config)
writer.writeFromXml(xmlFile, xmlParser)
```

### 4. Multi-File Loading
```scala
// Append multiple XMLs to same table
xmlFiles.foreach { file =>
  val writer = PostgreSQLWriter(schema, config)
  writer.writeFromXml(file, xmlParser)
}
```

## Benefits vs BigQuery

| Feature | PostgreSQL | BigQuery |
|---------|-----------|----------|
| Column Limit | None (JSONB) | 10,000 |
| Local Development | ✅ Easy | ❌ Emulator issues |
| JSONB Operators | ✅ Rich | ⚠️ Limited |
| Cost | ✅ Free (self-hosted) | $$$ Usage-based |
| Indexing | ✅ Flexible | ⚠️ Automatic only |
| Full-Text Search | ✅ Built-in | ⚠️ Limited |
| Transactions | ✅ Full ACID | ⚠️ Limited |

## Performance Tips

### Batch Inserts
The writer automatically batches 500 rows:
```scala
// Automatically handled
writer.write(rows.iterator)
```

### Connection Pooling
For production, use a connection pool:
```scala
// Example with HikariCP (add dependency)
val config = new HikariConfig()
config.setJdbcUrl(jdbcUrl)
config.setUsername(username)
config.setPassword(password)
val dataSource = new HikariDataSource(config)
```

### JSONB Best Practices
1. Use GIN indexes for JSONB columns
2. Extract frequently-queried fields to columns
3. Use materialized views for complex queries
4. Consider partitioning large tables

## Real-World Example

```sql
-- Create production table
CREATE TABLE music_releases (
  id SERIAL PRIMARY KEY,
  data JSONB NOT NULL,
  -- Extract key fields for indexing
  isrc TEXT GENERATED ALWAYS AS (
    (SELECT value FROM jsonb_array_elements(data) 
     WHERE value->>'name' = 'ISRC' LIMIT 1)
  ) STORED,
  artist TEXT GENERATED ALWAYS AS (
    (SELECT value FROM jsonb_array_elements(data) 
     WHERE value->>'name' = 'DisplayArtistName' LIMIT 1)
  ) STORED,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_music_isrc ON music_releases(isrc);
CREATE INDEX idx_music_artist ON music_releases(artist);
CREATE INDEX idx_music_data ON music_releases USING GIN(data);

-- Enable full-text search
ALTER TABLE music_releases 
ADD COLUMN search_vector tsvector
GENERATED ALWAYS AS (
  to_tsvector('english', data::text)
) STORED;

CREATE INDEX idx_music_search ON music_releases USING GIN(search_vector);
```

## Next Steps

1. Run the integration tests
2. Try loading your own DDEX files
3. Experiment with JSONB queries
4. Set up production PostgreSQL instance
5. Create custom views and indexes

## See Also

- [PostgreSQL JSONB Documentation](https://www.postgresql.org/docs/current/datatype-json.html)
- [JSONB Operators](https://www.postgresql.org/docs/current/functions-json.html)
- [Testcontainers](https://www.testcontainers.org/)
