# DDEX ERN to BigQuery Example

This example demonstrates loading DDEX ERN (Electronic Release Notification) XML files into BigQuery using the Firebase model approach.

## What is DDEX ERN?

DDEX ERN is an XML standard used by the music industry for exchanging release information between labels, distributors, and digital service providers. It contains:
- Release metadata (titles, artists, ISRCs, GRids)
- Sound recording details
- Territory-specific information
- Deal terms and rights information
- Complex nested structures with hundreds of possible fields

## Why Firebase Model?

DDEX ERN messages can have hundreds of different fields depending on the release type and completeness of metadata. Using the traditional columnar approach would:
- Hit BigQuery's 10,000 column limit
- Require schema updates for each DDEX version
- Make merging different message types difficult

The Firebase model solves this by storing data as key-value pairs.

## Files

- `out.xml` - DDEX ERN 3.8.2 message for a single track release
- `bout.xml` - Another DDEX ERN message (identical for testing)

## Running the Example

### Option 1: Automated Test Script

```bash
# Run the complete example with Docker emulator
./scripts/load-ddex-to-bigquery-local.sh
```

This will:
1. Start BigQuery emulator in Docker
2. Load both XML files using Firebase model
3. Run queries to extract music metadata
4. Demonstrate schema evolution
5. Clean up and stop emulator

### Option 2: Manual Steps

```bash
# 1. Start BigQuery emulator
just start-emulator

# 2. Load DDEX files (in another terminal)
slatr to-bigquery out.xml \
  --project test-project \
  --dataset music_metadata \
  --table release_notifications \
  --firebase-model

slatr to-bigquery bout.xml \
  --project test-project \
  --dataset music_metadata \
  --table release_notifications \
  --firebase-model \
  --write-mode append

# 3. Stop emulator when done
just stop-emulator
```

### Option 3: Run Integration Tests

```bash
# Run specific DDEX tests
sbt "integrationTests/testOnly *DdexToBigQuerySpec"

# Run all integration tests
sbt integrationTests/test
```

## What Gets Loaded

### Traditional Schema (Not Recommended)
Would create 100+ columns:
```
MessageId, MessageCreatedDateTime, ISRC, GRid, TitleText, 
DisplayArtistName, Duration, GenreText, TerritoryCode, ...
```

### Firebase Model Schema (Recommended)
Creates a simple, flexible structure:
```sql
CREATE TABLE music_metadata.release_notifications (
  fields ARRAY<STRUCT<
    name STRING,
    value STRING
  >>
)
```

## Querying the Data

### Find All Field Names
```sql
SELECT DISTINCT field.name
FROM `test-project.music_metadata.release_notifications`,
UNNEST(fields) AS field
ORDER BY field.name
```

### Extract Key Metadata
```sql
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'MessageId') as message_id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
  (SELECT value FROM UNNEST(fields) WHERE name LIKE '%TitleText%' LIMIT 1) as title,
  (SELECT value FROM UNNEST(fields) WHERE name = 'DisplayArtistName') as artist,
  (SELECT value FROM UNNEST(fields) WHERE name = 'Duration') as duration,
  (SELECT value FROM UNNEST(fields) WHERE name = 'GenreText') as genre
FROM `test-project.music_metadata.release_notifications`
```

### Create a View for Easier Access
```sql
CREATE VIEW music_metadata.releases_view AS
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'MessageId') as message_id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
  (SELECT value FROM UNNEST(fields) WHERE name = 'GRid') as grid,
  (SELECT value FROM UNNEST(fields) WHERE name LIKE '%TitleText%' LIMIT 1) as title,
  (SELECT value FROM UNNEST(fields) WHERE name = 'DisplayArtistName') as artist,
  (SELECT value FROM UNNEST(fields) WHERE name = 'Duration') as duration,
  (SELECT value FROM UNNEST(fields) WHERE name = 'GenreText') as genre
FROM `test-project.music_metadata.release_notifications`
```

### Find Releases by Artist
```sql
SELECT *
FROM `test-project.music_metadata.release_notifications`
WHERE EXISTS (
  SELECT 1 FROM UNNEST(fields) 
  WHERE name = 'DisplayArtistName' AND value = 'Example Artist'
)
```

### Find Releases by Genre
```sql
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name LIKE '%TitleText%' LIMIT 1) as title,
  (SELECT value FROM UNNEST(fields) WHERE name = 'GenreText') as genre
FROM `test-project.music_metadata.release_notifications`
WHERE EXISTS (
  SELECT 1 FROM UNNEST(fields) 
  WHERE name = 'GenreText' AND value = 'Rock'
)
```

## Expected Output

When you run the test, you should see:

```
🎵 Loading DDEX ERN XML files into BigQuery (Firebase model)
======================================================================

📁 Processing out.xml...
   ✓ Schema inferred: 145 fields
   ℹ️  Complex nested structure with:
      - MessageHeader, ResourceList, ReleaseList, DealList
      - SoundRecording metadata (ISRC, Duration, Titles)
      - Release information (GRid, Territory, Artists)
      - Deal terms and validity periods
   ✅ Loaded into BigQuery table: music_metadata.release_notifications

📊 Verifying BigQuery table structure...
   Table schema:
     - fields: STRUCT (REPEATED)

   Total rows loaded: 2

🔍 Discovering all field names in the DDEX data...
   Found 145+ distinct fields:
     - CommercialModelType
     - DisplayArtistName
     - Duration
     - FullName
     - GRid
     - GenreText
     - ISRC
     - LanguageOfPerformance
     - MessageCreatedDateTime
     - MessageId
     - PLineText
     - ParentalWarningType
     - PartyId
     - ReleaseType
     - ResourceReference
     - SequenceNumber
     - SoundRecordingType
     - TerritoryCode
     - TitleText
     - UseType
     ... and 125 more fields

🎼 Extracting music release information...

   Release Details:

     Message ID: 123456791
     ISRC: USAT12345678
     Title: Example Title Update
     Artist: Example Artist
     Duration: PT0H1M30S
     Genre: Rock
     Year: 2020

✅ Successfully loaded and queried DDEX ERN metadata!
```

## Real-World Use Cases

### Music Distribution Platform
Load DDEX messages from multiple labels without schema conflicts:
```bash
for file in ddex_messages/*.xml; do
  slatr to-bigquery "$file" \
    -p production-project \
    -d music_catalog \
    -t release_notifications \
    --firebase-model \
    --write-mode append
done
```

### Data Analytics
Create materialized views for specific queries:
```sql
-- Create a materialized table with commonly queried fields
CREATE TABLE music_metadata.releases_analytics AS
SELECT 
  (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
  (SELECT value FROM UNNEST(fields) WHERE name = 'DisplayArtistName') as artist,
  (SELECT value FROM UNNEST(fields) WHERE name = 'GenreText') as genre,
  CAST((SELECT value FROM UNNEST(fields) WHERE name = 'Year') AS INT64) as year
FROM `test-project.music_metadata.release_notifications`
```

### Schema Discovery
Find all possible field names across your entire catalog:
```sql
SELECT 
  field.name,
  COUNT(*) as occurrence_count,
  COUNT(DISTINCT(SELECT value FROM UNNEST(fields) WHERE name = field.name)) as unique_values
FROM `test-project.music_metadata.release_notifications`,
UNNEST(fields) AS field
GROUP BY field.name
ORDER BY occurrence_count DESC
```

## Benefits Demonstrated

1. ✅ **No Column Limits** - 145+ fields stored without issues
2. ✅ **Schema Evolution** - Can append messages with different field sets
3. ✅ **Flexible Querying** - Use UNNEST to extract any field
4. ✅ **No Schema Management** - New DDEX versions work automatically
5. ✅ **Data Discovery** - Easily find all available fields
6. ✅ **Production Ready** - Tested with real DDEX ERN 3.8.2 format

## Next Steps

- Load your own DDEX files
- Create custom views for your use cases
- Set up scheduled ingestion pipelines
- Build dashboards on top of the data
- Implement data quality checks
