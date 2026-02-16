# DDEX XML Data: PostgreSQL JSONB Querying Guide

## Overview
These PostgreSQL queries demonstrate how to extract and analyze DDEX XML data stored as JSONB.

## Prerequisites
- Ensure data is loaded using `LoadToPostgres`
- Table: `ddex_releases` with JSONB column `data`

## Basic Queries

### 1. Total Records
```sql
SELECT COUNT(*) FROM ddex_releases;
```

### 2. Release Dates Distribution
```sql
SELECT 
    data->'MessageCreatedDateTime'->0->>'#text' as release_date, 
    COUNT(*) as release_count
FROM ddex_releases
WHERE data ? 'MessageCreatedDateTime'
GROUP BY release_date
ORDER BY release_count DESC;
```

### 3. Sender Analysis
```sql
-- Unique Senders
SELECT DISTINCT 
    data->'MessageSender'->0->'PartyName'->0->'FullName'->0->>'#text' as sender
FROM ddex_releases
WHERE data ? 'MessageSender'
ORDER BY sender;

-- Releases by Sender
SELECT 
    data->'MessageSender'->0->'PartyName'->0->'FullName'->0->>'#text' as sender,
    COUNT(*) as release_count
FROM ddex_releases
WHERE data ? 'MessageSender'
GROUP BY sender
ORDER BY release_count DESC;
```

## Advanced Queries

### 4. Full-Text Search
```sql
-- Search across entire JSONB data
SELECT _id, data
FROM ddex_releases
WHERE data::text LIKE '%UniversalMusicGroup%';
```

### 5. Aggregations
```sql
-- Release Distribution by Year
SELECT 
    EXTRACT(YEAR FROM (data->'MessageCreatedDateTime'->0->>'#text')::timestamp) as release_year,
    COUNT(*) as yearly_releases
FROM ddex_releases
WHERE data ? 'MessageCreatedDateTime'
GROUP BY release_year
ORDER BY yearly_releases DESC;
```

## Performance Tips

### Indexing
```sql
-- Create GIN indexes for faster JSONB queries
CREATE INDEX idx_ddex_sender 
ON ddex_releases 
USING gin ((data->'MessageSender'));

CREATE INDEX idx_ddex_soundrecording 
ON ddex_releases 
USING gin ((data->'SoundRecording'));
```

## JSONB Operator Cheatsheet
- `->`: Extract JSON object field
- `->>`: Extract JSON object field as text
- `?`: Check if key exists
- `@>`: Contains
- `?|`: Any key exists
- `?&`: All keys exist

## Troubleshooting
- Use `jsonb_pretty(data)` to format complex JSONB
- Use `::text` to convert JSONB to text for debugging
- Always use `?` to check key existence before nested extraction