# Firebase Model for BigQuery

## Overview

The Firebase model is an alternative schema approach for BigQuery that stores data as an array of key-value structs instead of traditional columns. This approach is inspired by Firebase's data model and provides several benefits for XML data ingestion.

## Benefits

### 1. **No Column Limit Issues**
- Traditional BigQuery tables are limited to 10,000 columns
- Firebase model uses a single repeated STRUCT field
- Can handle XML files with thousands of different elements

### 2. **Schema Evolution**
- No need to alter table schema when XML structure changes
- New fields are automatically accommodated
- Different XML versions can coexist in the same table

### 3. **Flexible Merging**
- Easily merge data from slightly different XML schemas
- No schema conflicts when combining datasets
- Simplified data integration

### 4. **Denormalization Support**
- Easy to query and unnest specific fields
- Flexible aggregation and filtering
- Better suited for analytical workloads on variable schemas

## Schema Structure

### Firebase Model Schema
```sql
CREATE TABLE dataset.table (
  fields ARRAY<STRUCT<
    name STRING,
    value STRING
  >>
)
```

### Traditional Schema
```sql
CREATE TABLE dataset.table (
  field1 STRING,
  field2 INT64,
  field3 TIMESTAMP,
  -- ... up to 10,000 columns
)
```

## Usage

### CLI Command

```bash
# Use Firebase model
slatr to-bigquery input.xml \
  --project my-project \
  --dataset my-dataset \
  --table my-table \
  --firebase-model

# Traditional approach (default)
slatr to-bigquery input.xml \
  --project my-project \
  --dataset my-dataset \
  --table my-table
```

### Example Data

#### Input XML
```xml
<record>
  <id>123</id>
  <name>John Doe</name>
  <email>john@example.com</email>
  <age>30</age>
</record>
```

#### Firebase Model Output
```json
{
  "fields": [
    {"name": "id", "value": "123"},
    {"name": "name", "value": "John Doe"},
    {"name": "email", "value": "john@example.com"},
    {"name": "age", "value": "30"}
  ]
}
```

#### Traditional Model Output
```json
{
  "id": "123",
  "name": "John Doe",
  "email": "john@example.com",
  "age": 30
}
```

## Querying Firebase Model Data

### Extract Specific Fields

```sql
SELECT
  (SELECT value FROM UNNEST(fields) WHERE name = 'id') AS id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'name') AS name,
  (SELECT value FROM UNNEST(fields) WHERE name = 'email') AS email
FROM `project.dataset.table`
```

### Create View for Easy Access

```sql
CREATE VIEW `project.dataset.table_view` AS
SELECT
  (SELECT value FROM UNNEST(fields) WHERE name = 'id') AS id,
  (SELECT value FROM UNNEST(fields) WHERE name = 'name') AS name,
  (SELECT value FROM UNNEST(fields) WHERE name = 'email') AS email,
  (SELECT value FROM UNNEST(fields) WHERE name = 'age') AS age
FROM `project.dataset.table`
```

### Find All Distinct Field Names

```sql
SELECT DISTINCT field.name
FROM `project.dataset.table`,
UNNEST(fields) AS field
ORDER BY field.name
```

### Filter by Field Value

```sql
SELECT *
FROM `project.dataset.table`
WHERE EXISTS (
  SELECT 1 FROM UNNEST(fields) 
  WHERE name = 'status' AND value = 'active'
)
```

## When to Use Firebase Model

### Use Firebase Model When:
- XML files have highly variable schemas
- You expect schema changes over time
- Merging data from multiple XML sources with different structures
- Column count approaches or exceeds BigQuery limits
- Schema flexibility is more important than query performance

### Use Traditional Model When:
- XML schema is stable and well-defined
- Query performance is critical
- You need strong typing (INT64, TIMESTAMP, etc.)
- Column count is well under 10,000
- BI tools require traditional column structure

## Performance Considerations

### Firebase Model
- **Pros**: Flexible, no schema management, handles any XML structure
- **Cons**: Requires UNNEST in queries, all values stored as strings, slightly slower queries

### Traditional Model
- **Pros**: Fast queries, strong typing, native BI tool support
- **Cons**: Schema management overhead, column limits, schema evolution challenges

## Best Practices

1. **Create Views**: Define SQL views on top of Firebase model tables for easier querying
2. **Index Strategy**: Consider materializing frequently-accessed views
3. **Type Conversion**: Convert string values to appropriate types in views
4. **Documentation**: Document expected field names and value formats
5. **Partitioning**: Use partitioning by ingestion time for better query performance

## Example: Handling Multiple XML Versions

```bash
# Version 1 XML with fields: id, name, email
slatr to-bigquery data_v1.xml \
  --project my-project \
  --dataset my-dataset \
  --table unified_data \
  --firebase-model

# Version 2 XML with fields: id, name, email, phone, address
slatr to-bigquery data_v2.xml \
  --project my-project \
  --dataset my-dataset \
  --table unified_data \
  --firebase-model \
  --write-mode append

# Both versions coexist in the same table without schema conflicts!
```

## Migration from Traditional to Firebase Model

If you have existing tables and want to migrate:

```sql
-- Create new Firebase model table from traditional table
CREATE TABLE `project.dataset.table_firebase` AS
SELECT
  ARRAY(
    SELECT AS STRUCT 'field1' AS name, CAST(field1 AS STRING) AS value UNION ALL
    SELECT AS STRUCT 'field2' AS name, CAST(field2 AS STRING) AS value UNION ALL
    SELECT AS STRUCT 'field3' AS name, CAST(field3 AS STRING) AS value
  ) AS fields
FROM `project.dataset.table_traditional`
```
