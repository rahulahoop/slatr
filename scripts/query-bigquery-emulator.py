#!/usr/bin/env python3
"""
Query BigQuery emulator for DDEX data

Prerequisites:
    pip install google-cloud-bigquery

Usage:
    # Start emulator first
    ./scripts/load-ddex-to-bigquery-local.sh skip-tests
    
    # Then run queries
    python3 scripts/query-bigquery-emulator.py
"""

from google.cloud import bigquery
from google.api_core import client_options
from google.auth.credentials import AnonymousCredentials
import sys

def create_client():
    """Create BigQuery client pointing to local emulator"""
    options = client_options.ClientOptions(
        api_endpoint="http://localhost:9050"
    )
    
    client = bigquery.Client(
        project="test-project",
        client_options=options,
        credentials=AnonymousCredentials()
    )
    
    return client

def list_tables(client):
    """List all tables in the dataset"""
    print("📋 Available tables:")
    print("-" * 60)
    
    dataset_id = "test-project.music_metadata"
    
    try:
        tables = client.list_tables(dataset_id)
        for table in tables:
            print(f"   {table.table_id}")
            
            # Get table info
            table_ref = client.get_table(f"{dataset_id}.{table.table_id}")
            print(f"      Rows: {table_ref.num_rows}")
            print(f"      Schema: {len(table_ref.schema)} fields")
            print()
    except Exception as e:
        print(f"   ❌ Error: {e}")
        print(f"   Make sure the emulator is running and tests have been executed")
        return False
    
    return True

def query_firebase_model(client):
    """Query Firebase model data"""
    print("\n🔍 Querying Firebase model data:")
    print("-" * 60)
    
    # Count total rows
    query = """
        SELECT COUNT(*) as count 
        FROM `test-project.music_metadata.release_notifications`
    """
    
    try:
        results = client.query(query).result()
        for row in results:
            print(f"   Total rows: {row['count']}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return
    
    # Get distinct field names
    query = """
        SELECT DISTINCT field.name as field_name
        FROM `test-project.music_metadata.release_notifications`,
        UNNEST(fields) AS field
        ORDER BY field_name
        LIMIT 20
    """
    
    print(f"\n   Field names (first 20):")
    try:
        results = client.query(query).result()
        for row in results:
            print(f"     - {row['field_name']}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Extract music metadata
    query = """
        SELECT 
          (SELECT value FROM UNNEST(fields) WHERE name = 'MessageId') as message_id,
          (SELECT value FROM UNNEST(fields) WHERE name = 'ISRC') as isrc,
          (SELECT value FROM UNNEST(fields) WHERE name LIKE '%TitleText%' LIMIT 1) as title,
          (SELECT value FROM UNNEST(fields) WHERE name = 'DisplayArtistName') as artist,
          (SELECT value FROM UNNEST(fields) WHERE name = 'GenreText') as genre
        FROM `test-project.music_metadata.release_notifications`
        LIMIT 5
    """
    
    print(f"\n   🎵 Music releases:")
    try:
        results = client.query(query).result()
        for row in results:
            print(f"")
            print(f"     Message ID: {row['message_id'] or 'N/A'}")
            print(f"     ISRC: {row['isrc'] or 'N/A'}")
            print(f"     Title: {row['title'] or 'N/A'}")
            print(f"     Artist: {row['artist'] or 'N/A'}")
            print(f"     Genre: {row['genre'] or 'N/A'}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

def run_custom_query(client, sql):
    """Run a custom SQL query"""
    print(f"\n🔍 Custom query:")
    print("-" * 60)
    print(f"SQL: {sql}")
    print()
    
    try:
        results = client.query(sql).result()
        
        # Print results
        for i, row in enumerate(results):
            if i == 0:
                # Print header
                print("   ", " | ".join(row.keys()))
                print("   ", "-" * 60)
            
            # Print row
            values = [str(v) for v in row.values()]
            print("   ", " | ".join(values))
            
            if i >= 9:  # Limit to 10 rows
                break
                
    except Exception as e:
        print(f"   ❌ Error: {e}")

def main():
    print("🎵 BigQuery Emulator Query Tool")
    print("=" * 60)
    print()
    
    try:
        client = create_client()
        print("✅ Connected to BigQuery emulator at http://localhost:9050")
        print()
    except Exception as e:
        print(f"❌ Failed to connect to emulator: {e}")
        print()
        print("Make sure:")
        print("  1. Emulator is running: ./scripts/load-ddex-to-bigquery-local.sh skip-tests")
        print("  2. Port 9050 is accessible")
        sys.exit(1)
    
    # List tables
    if not list_tables(client):
        sys.exit(1)
    
    # Query Firebase model
    query_firebase_model(client)
    
    # Interactive mode
    print("\n" + "=" * 60)
    print("💡 Interactive query mode")
    print("   Enter SQL queries or 'quit' to exit")
    print("=" * 60)
    
    while True:
        try:
            query = input("\nSQL> ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                break
                
            if not query:
                continue
                
            run_custom_query(client, query)
            
        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except EOFError:
            break

if __name__ == "__main__":
    main()
