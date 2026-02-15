# PostgreSQL Playground

A local PostgreSQL instance for exploring slatr's XML-to-database capabilities.
Pre-seeded with sample DDEX music metadata in both the **Firebase JSONB model** and
a traditional columnar layout.

## Quick Start

```bash
# Start (pulls image on first run, seeds sample data automatically)
just pg-start

# Open a SQL shell
just pg-psql

# Run a query
just pg-query "SELECT * FROM ddex_releases_flat;"

# Load ALL example XMLs (firebase JSONB model -- recommended)
just pg-load-all

# Load ALL example XMLs (traditional columnar model)
just pg-load-all mode=traditional

# Load a single file
just pg-load examples/out.xml

# Stop (data persists across restarts)
just pg-stop

# Destroy data and start fresh
just pg-reset
```

## Connection Details

| Parameter | Value |
|-----------|-------|
| Host      | `localhost` |
| Port      | `5432` |
| Database  | `music_metadata` |
| Username  | `slatr` |
| Password  | `slatr` |
| JDBC URL  | `jdbc:postgresql://localhost:5432/music_metadata` |

## Connecting with DBeaver

1. Open DBeaver and click **Database > New Database Connection** (or the plug icon).
2. Select **PostgreSQL** and click **Next**.
3. Fill in the connection form:

   | Field    | Value |
   |----------|-------|
   | Host     | `localhost` |
   | Port     | `5432` |
   | Database | `music_metadata` |
   | Username | `slatr` |
   | Password | `slatr` |

4. Click **Test Connection** to verify. If prompted to download the PostgreSQL
   JDBC driver, click **Download**.
5. Click **Finish**.

You should now see the `music_metadata` database in the Database Navigator with:

- **Tables**: `ddex_releases`, `products`
- **Views**: `ddex_releases_flat`

### Useful DBeaver Tips

- Double-click `ddex_releases_flat` to browse the flattened DDEX data.
- Open an SQL editor (**SQL Editor > New SQL Script**) and try the queries below.
- Right-click a table > **View Diagram** to see the schema visually.

## What's Inside

### Tables

#### `ddex_releases` (Firebase JSONB model)
```
 _id | data (JSONB)                                           | created_at
-----+--------------------------------------------------------+-------------------
   1 | [{"name":"ISRC","value":"USAT20001234"}, ...]           | 2026-02-15 ...
   2 | [{"name":"ISRC","value":"GBAYE0500001"}, ...]           | 2026-02-15 ...
   3 | [{"name":"ISRC","value":"USAT20001235"}, ...]           | 2026-02-15 ...
```

Each row stores all XML fields as a JSONB array of `{"name": "...", "value": "..."}`
objects. This is slatr's "Firebase model" -- it avoids the column explosion problem
with wide XML schemas (DDEX ERN messages can have 500+ fields).

#### `products` (traditional columnar model)
```
 _id | id | name                | price  | available | created_at
-----+----+---------------------+--------+-----------+-----------
   1 |  1 | Wireless Headphones |  79.99 | t         | ...
   2 |  2 | USB-C Cable         |  12.49 | t         | ...
```

A standard relational table showing how slatr maps XML fields to typed columns.

### Views

#### `ddex_releases_flat`
A convenience view that extracts common DDEX fields from the JSONB data:

```
 _id | message_id | isrc         | title         | artist       | genre
-----+------------+--------------+---------------+--------------+-----------
   1 | MSG001     | USAT20001234 | Summer Breeze | The Coastals | Pop
   2 | MSG002     | GBAYE0500001 | Midnight Run  | Neon Pulse   | Electronic
   3 | MSG003     | USAT20001235 | Winter Chill  | The Coastals | Ambient
```

## Example Queries

### Basic JSONB queries

```sql
-- All releases with their field count
SELECT _id,
       jsonb_array_length(data) AS field_count,
       created_at
FROM ddex_releases;

-- Find a release by ISRC
SELECT * FROM ddex_releases
WHERE data @> '[{"name":"ISRC","value":"USAT20001234"}]';

-- Search across all field values
SELECT * FROM ddex_releases
WHERE data::text ILIKE '%coastals%';
```

### Extract individual fields from JSONB

```sql
-- Pull specific named values out of the array
SELECT
  _id,
  (SELECT elem->>'value'
   FROM jsonb_array_elements(data) AS elem
   WHERE elem->>'name' = 'ISRC') AS isrc,
  (SELECT elem->>'value'
   FROM jsonb_array_elements(data) AS elem
   WHERE elem->>'name' = 'DisplayArtistName') AS artist
FROM ddex_releases;
```

### Cross-table join (traditional + JSONB)

```sql
-- Just an example of mixing models
SELECT p.name AS product,
       d.isrc
FROM products p
CROSS JOIN ddex_releases_flat d
WHERE p.available = true
LIMIT 10;
```

### List every distinct field name across all releases

```sql
SELECT DISTINCT elem->>'name' AS field_name
FROM ddex_releases,
     jsonb_array_elements(data) AS elem
ORDER BY field_name;
```

## Loading XML Files

### Load all examples at once

```bash
# Firebase JSONB model (recommended -- works with all XML files)
just pg-load-all

# Traditional columnar model (works with simple XMLs; complex DDEX
# schemas may fail due to nested structures)
just pg-load-all mode=traditional
```

Each XML file gets its own table named after the file (`out.xml` -> `out`,
`42_Audio.xml` -> `ddex_42_audio`, etc.).

### Load a single file

```bash
just pg-load examples/out.xml
just pg-load examples/bout.xml mode=traditional
```

### Direct script usage

```bash
# Load all examples with firebase model
./scripts/load-ddex-to-postgres-local.sh --all --firebase

# Load all into traditional columns
./scripts/load-ddex-to-postgres-local.sh --all --traditional

# Load into a specific table
./scripts/load-ddex-to-postgres-local.sh --all --table all_releases

# Single file with env overrides
PGTABLE=my_table ./scripts/load-ddex-to-postgres-local.sh path/to/file.xml
```

### Firebase vs Traditional

| | Firebase (JSONB) | Traditional (columns) |
|---|---|---|
| Model | `data JSONB` column with `[{"name":"...","value":"..."}]` | One column per XML field |
| Complex DDEX | Works for all files (25/25) | Fails for deeply-nested XML |
| Simple XML | Works | Works |
| Querying | JSONB operators (`@>`, `->>`), views | Standard SQL |
| Schema evolution | Automatic (different fields per row) | Requires ALTER TABLE |

Use **firebase** for DDEX and complex XML. Use **traditional** for simple,
flat XML files where you want typed columns.

## Environment Variables

The docker-compose setup uses these defaults. Override them in a `.env` file or
export them before running:

| Variable     | Default          | Description |
|--------------|------------------|-------------|
| `PGHOST`     | `localhost`      | PostgreSQL host |
| `PGPORT`     | `5432`           | PostgreSQL port |
| `PGDATABASE` | `music_metadata` | Database name |
| `PGUSER`     | `slatr`          | Username |
| `PGPASSWORD` | `slatr`          | Password |
| `PGTABLE`    | `ddex_releases`  | Target table for XML loading |

## Troubleshooting

**Container won't start**
```bash
# Check if port 5432 is already in use
lsof -i :5432
# Stop any existing postgres
brew services stop postgresql@16  # if using Homebrew
```

**Permission denied on init script**
```bash
chmod +r scripts/postgres-init.sql
```

**Data disappeared after restart**
Data is stored in a Docker volume (`pgdata`). It survives `docker compose down`
but is destroyed by `docker compose down -v` or `just pg-reset`.

**Want to connect from another tool (DataGrip, TablePlus, pgAdmin, etc.)**
Use the same connection details as DBeaver above. Any PostgreSQL client that
supports JDBC or libpq will work.
