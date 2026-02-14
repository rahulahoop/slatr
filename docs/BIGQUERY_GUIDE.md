# BigQuery Integration Guide

This guide explains how to use slatr to stream XML data directly into Google BigQuery tables with automatically inferred schemas.

## Overview

slatr can automatically:
1. Infer schema from your XML files
2. Create BigQuery tables with the correct schema
3. Stream data directly to BigQuery
4. Handle nested structures and arrays

## Prerequisites

### 1. Google Cloud Setup

```bash
# Install Google Cloud SDK
# https://cloud.google.com/sdk/docs/install

# Authenticate
gcloud auth login
gcloud auth application-default login

# Set project
gcloud config set project YOUR_PROJECT_ID
```

### 2. Create Dataset (if needed)

```bash
bq mk --dataset YOUR_PROJECT_ID:your_dataset
```

## Quick Start

### 1. Dry Run (Preview Schema)

Before inserting data, preview the inferred schema:

```bash
java -jar slatr.jar to-bigquery input.xml \
  --project my-project \
  --dataset my_dataset \
  --table my_table \
  --dry-run
```

**Output:**
```
Processing input.xml -> BigQuery: my-project.my_dataset.my_table
Inferring schema from XML...
✓ Schema inferred: 5 fields

Schema:
  title: StringType?
  author: StringType?
  year: IntType?
  price: DoubleType?
  available: BooleanType?

Dry run complete - no data inserted
```

### 2. Insert Data

```bash
java -jar slatr.jar to-bigquery input.xml \
  --project my-project \
  --dataset my_dataset \
  --table my_table
```

## Authentication

### Option 1: Application Default Credentials (Recommended)

```bash
# Set up Application Default Credentials
gcloud auth application-default login

# Run slatr (will use ADC automatically)
java -jar slatr.jar to-bigquery input.xml \
  --project my-project \
  --dataset my_dataset \
  --table my_table
```

### Option 2: Service Account

```bash
# Download service account key from GCP Console
# IAM & Admin > Service Accounts > Create Key (JSON)

# Use with slatr
java -jar slatr.jar to-bigquery input.xml \
  --project my-project \
  --dataset my_dataset \
  --table my_table \
  --credentials /path/to/service-account.json
```

**Required IAM Permissions:**
- `bigquery.tables.create` - For creating tables
- `bigquery.tables.updateData` - For inserting data
- `bigquery.datasets.get` - For checking dataset existence

## CLI Reference

### Command

```bash
slatr to-bigquery <input-xml> [options]
```

### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--project` | `-p` | GCP project ID | Required |
| `--dataset` | `-d` | BigQuery dataset name | Required |
| `--table` | `-t` | BigQuery table name | Required |
| `--location` | | BigQuery location | `US` |
| `--write-mode` | | Write mode: append, overwrite, error | `append` |
| `--create-table` | | Create table if doesn't exist | `true` |
| `--credentials` | | Path to service account JSON | None (uses ADC) |
| `--validate` | | Validate XML against XSD | `false` |
| `--dry-run` | | Preview schema only, don't insert | `false` |

## Write Modes

### Append (Default)

Add rows to existing table:

```bash
java -jar slatr.jar to-bigquery input.xml \
  -p my-project -d my_dataset -t my_table \
  --write-mode append
```

### Overwrite

Delete all existing data and insert new:

```bash
java -jar slatr.jar to-bigquery input.xml \
  -p my-project -d my_dataset -t my_table \
  --write-mode overwrite
```

**Warning:** This executes `DELETE FROM table WHERE TRUE` before inserting!

### Error If Exists

Fail if table already has data:

```bash
java -jar slatr.jar to-bigquery input.xml \
  -p my-project -d my_dataset -t my_table \
  --write-mode error
```

## Type Mapping

slatr automatically maps XML data types to BigQuery types:

| slatr Type | BigQuery Type | Example XML |
|------------|---------------|-------------|
| StringType | STRING | `<name>John</name>` |
| IntType | INT64 | `<age>25</age>` |
| LongType | INT64 | `<id>1234567890</id>` |
| DoubleType | FLOAT64 | `<price>19.99</price>` |
| BooleanType | BOOL | `<active>true</active>` |
| DateType | DATE | `<date>2024-01-15</date>` |
| TimestampType | TIMESTAMP | `<timestamp>2024-01-15T10:30:00Z</timestamp>` |
| TimeType | TIME | `<time>10:30:00</time>` |
| DecimalType | NUMERIC | `<amount>1234.56</amount>` |
| ArrayType | REPEATED | `<tags><tag>a</tag><tag>b</tag></tags>` |

## Array Handling

slatr correctly handles XML arrays by using BigQuery's REPEATED mode:

**XML:**
```xml
<record>
  <tags>
    <tag>important</tag>
    <tag>urgent</tag>
  </tags>
</record>
```

**BigQuery Schema:**
```sql
CREATE TABLE my_table (
  tags ARRAY<STRING>
)
```

**Query:**
```sql
SELECT tags FROM my_table
-- Result: ['important', 'urgent']
```

## Field Name Cleaning

BigQuery has strict field name requirements. slatr automatically cleans names:

| XML Field | BigQuery Field | Rule |
|-----------|----------------|------|
| `#text` | `text` | Remove # |
| `@attribute` | `attr_attribute` | Prefix with attr_ |
| `field.with.dots` | `field_with_dots` | Replace dots with _ |
| `field-with-dashes` | `field_with_dashes` | Replace dashes with _ |

## Examples

### Example 1: Simple XML to BigQuery

**Input (books.xml):**
```xml
<catalog>
  <book>
    <title>The Great Gatsby</title>
    <author>F. Scott Fitzgerald</author>
    <year>1925</year>
    <price>12.99</price>
  </book>
</catalog>
```

**Command:**
```bash
java -jar slatr.jar to-bigquery books.xml \
  -p my-project -d library -t books
```

**Generated BigQuery Schema:**
```sql
CREATE TABLE books (
  title STRING,
  author STRING,
  year INT64,
  price FLOAT64
)
```

**Query:**
```sql
SELECT * FROM `my-project.library.books`
```

### Example 2: Nested XML

**Input (employees.xml):**
```xml
<company>
  <employee>
    <id>1</id>
    <name>John Doe</name>
    <contact>
      <email>john@example.com</email>
      <phone>555-0001</phone>
    </contact>
  </employee>
</company>
```

**Command:**
```bash
java -jar slatr.jar to-bigquery employees.xml \
  -p my-project -d hr -t employees
```

**BigQuery Schema:**
```sql
CREATE TABLE employees (
  id INT64,
  name STRING,
  contact_email STRING,  -- Flattened from nested structure
  contact_phone STRING
)
```

### Example 3: With XSD Validation

```bash
java -jar slatr.jar to-bigquery input.xml \
  -p my-project -d my_dataset -t my_table \
  --validate
```

This will:
1. Download XSD from XML header
2. Validate XML against XSD
3. Use XSD types for more accurate schema inference
4. Insert data to BigQuery

## Querying Your Data

After loading data, query it with standard SQL:

```sql
-- View all data
SELECT * FROM `my-project.my_dataset.my_table` LIMIT 10;

-- Aggregate
SELECT 
  author,
  COUNT(*) as book_count,
  AVG(price) as avg_price
FROM `my-project.my_dataset.my_table`
GROUP BY author;

-- Array operations
SELECT 
  title,
  tag
FROM `my-project.my_dataset.my_table`,
UNNEST(tags) as tag;
```

## Performance Tips

### 1. Batch Size

slatr inserts data in batches of 500 rows (BigQuery recommended size). For very large files, consider:

```bash
# Split large XML first, then process in parallel
split -l 10000 large.xml chunk_

for file in chunk_*; do
  java -jar slatr.jar to-bigquery $file \
    -p my-project -d my_dataset -t my_table &
done
wait
```

### 2. Partitioning

For large tables, create partitioned tables:

```sql
CREATE TABLE `my-project.my_dataset.my_table`
PARTITION BY DATE(timestamp_field)
AS SELECT * FROM `my-project.my_dataset.my_table_temp`;
```

### 3. Clustering

Add clustering for better query performance:

```sql
CREATE TABLE `my-project.my_dataset.my_table`
PARTITION BY DATE(timestamp_field)
CLUSTER BY user_id, category
AS SELECT * FROM `my-project.my_dataset.my_table_temp`;
```

## Troubleshooting

### Error: "Table already exists"

```bash
# Use --write-mode to handle existing tables
java -jar slatr.jar to-bigquery input.xml \
  -p my-project -d my_dataset -t my_table \
  --write-mode append  # or overwrite
```

### Error: "Permission denied"

```bash
# Ensure service account has required permissions
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:my-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

### Error: "Invalid field name"

Field names are automatically cleaned, but if you see this error, check for:
- Very long field names (>300 chars)
- Empty field names
- Conflicting names after cleaning

### Error: "Quota exceeded"

BigQuery has quotas on:
- 1,000 inserts per second per table
- 10 GB per second per project

For large volumes, consider:
1. Using Cloud Storage → BigQuery load instead
2. Converting to Parquet first, then loading to BigQuery

## Integration with Data Pipelines

### Apache Airflow

```python
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG('xml_to_bigquery', schedule_interval='@daily') as dag:
    load_xml = BashOperator(
        task_id='load_xml_to_bq',
        bash_command='''
            java -jar /path/to/slatr.jar to-bigquery \
                {{ params.input_file }} \
                -p {{ params.project }} \
                -d {{ params.dataset }} \
                -t {{ params.table }} \
                --credentials {{ params.credentials }}
        ''',
        params={
            'input_file': '/data/daily_export.xml',
            'project': 'my-project',
            'dataset': 'my_dataset',
            'table': 'daily_data',
            'credentials': '/secrets/gcp-sa.json'
        }
    )
```

### Cloud Functions

```javascript
const {Storage} = require('@google-cloud/storage');
const {exec} = require('child_process');

exports.xmlToBigQuery = async (file, context) => {
  const command = `java -jar slatr.jar to-bigquery \
    gs://${file.bucket}/${file.name} \
    -p ${process.env.PROJECT_ID} \
    -d ${process.env.DATASET} \
    -t ${process.env.TABLE}`;
  
  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error(`Error: ${error}`);
      return;
    }
    console.log(`Success: ${stdout}`);
  });
};
```

## Best Practices

1. **Always dry-run first** - Preview the schema before inserting data
2. **Use service accounts** - For production, use service accounts with minimal permissions
3. **Monitor costs** - BigQuery charges for data stored and queries run
4. **Partition large tables** - Use date partitioning for time-series data
5. **Test with small files** - Validate your pipeline with small XML files first
6. **Handle errors gracefully** - Implement retry logic for transient failures
7. **Version your schemas** - Keep track of schema changes over time

## Limitations

- **Nested structures**: Complex nested XML is flattened (nested text extracted)
- **Batch size**: Fixed at 500 rows per batch
- **No streaming API**: Uses insertAll API, not streaming API
- **Schema evolution**: Table schema must match inferred schema
- **No chunking yet**: Large files processed in memory (chunking coming in future release)

## Next Steps

- Try the [Parquet Guide](PARQUET_GUIDE.md) for better performance with Spark
- Learn about [Schema Inference](SCHEMA_INFERENCE.md)
- Explore [Spark Integration](SPARK_GUIDE.md) (coming soon)
