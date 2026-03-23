"""
Load TransXChange data from JSON to PostgreSQL
Optimized with batch processing for maximum speed
"""

import psycopg2
import psycopg2.extras
import json
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Connect to database
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT", 5432)
)
conn.autocommit = False
cur = conn.cursor()

print("Loading TransXChange data...")
print("="*80)

# Load JSON file
json_path = "../static/liverpool_transit_data_enriched.json"
print(f"\n1. Reading {json_path}...")

with open(json_path, 'r') as f:
    data = json.load(f)

stops = data['stops']
operators = data['operators']

# Flatten routes from all operators
routes = []
for op_name, op_data in operators.items():
    for route in op_data['routes']:
        route['operator_name'] = op_data['full_name']
        route['operator_noc'] = op_data.get('noc')
        routes.append(route)

print(f"   ✓ Loaded {len(routes):,} routes from {len(operators)} operators, {len(stops):,} stops")

# Step 1: Load stops using execute_values (fastest method)
print("\n2. Inserting stops (batch insert)...")
stop_values = [
    (naptan_id, stop['name'], stop['lat'], stop['lon'])
    for naptan_id, stop in stops.items()
    if stop.get('lat') is not None and stop.get('lon') is not None
]

skipped = len(stops) - len(stop_values)
if skipped > 0:
    print(f"   ⚠ Skipping {skipped} stops with missing coordinates")

print(f"   Executing batch insert of {len(stop_values):,} stops...")
psycopg2.extras.execute_values(
    cur,
    """
    INSERT INTO txc_stops (naptan_id, stop_name, latitude, longitude)
    VALUES %s
    ON CONFLICT (naptan_id) DO NOTHING
    """,
    stop_values,
    page_size=1000
)
conn.commit()
print(f"   ✓ Inserted {len(stop_values):,} stops")

# Step 2: Load route patterns using execute_values
print("\n3. Inserting route patterns (batch insert)...")
pattern_values = [
    (
        route['service_code'],
        route['operator_name'],
        route.get('operator_noc'),
        route['route_name'],
        route.get('direction'),
        route.get('description'),
        None
    )
    for route in routes
]

print(f"   Executing batch insert of {len(pattern_values):,} patterns...")
psycopg2.extras.execute_values(
    cur,
    """
    INSERT INTO txc_route_patterns (
        service_code, operator_name, operator_noc,
        route_name, direction, origin, destination
    )
    VALUES %s
    ON CONFLICT (service_code) DO NOTHING
    """,
    pattern_values,
    page_size=1000
)
conn.commit()
print(f"   ✓ Inserted {len(pattern_values):,} route patterns")

# Step 3: Load stop sequences using execute_values in chunks
print("\n4. Inserting stop sequences (batch processing)...")

# Collect all sequences first (only for stops that exist)
valid_stops = set(naptan_id for naptan_id, stop in stops.items() 
                  if stop.get('lat') is not None and stop.get('lon') is not None)

all_sequences = []
skipped_sequences = 0

for route in routes:
    service_code = route['service_code']
    for idx, naptan_id in enumerate(route['stops']):
        # stops are just naptan_id strings
        if naptan_id in valid_stops:
            all_sequences.append((service_code, naptan_id, idx + 1))
        else:
            skipped_sequences += 1

if skipped_sequences > 0:
    print(f"   ⚠ Skipping {skipped_sequences} sequences for stops without coordinates")

print(f"   Total sequences to insert: {len(all_sequences):,}")

# Insert in chunks of 10000
chunk_size = 10000
total_processed = 0

for i in range(0, len(all_sequences), chunk_size):
    chunk = all_sequences[i:i+chunk_size]
    
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO txc_pattern_stops (service_code, naptan_id, stop_sequence)
        VALUES %s
        ON CONFLICT (service_code, stop_sequence) DO NOTHING
        """,
        chunk,
        page_size=1000
    )
    
    total_processed += len(chunk)
    print(f"   Progress: {total_processed:,}/{len(all_sequences):,} sequences...", end='\r')
    sys.stdout.flush()
    
    # Commit every chunk
    conn.commit()

print(f"\n   ✓ Inserted {len(all_sequences):,} stop sequences")

# Verify data
print("\n" + "="*80)
print("VERIFICATION")
print("="*80)

cur.execute("SELECT COUNT(*) FROM txc_stops")
print(f"\nStops: {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM txc_route_patterns")
print(f"Route patterns: {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM txc_pattern_stops")
print(f"Stop sequences: {cur.fetchone()[0]:,}")

cur.execute("""
    SELECT 
        tablename,
        pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size
    FROM pg_tables
    WHERE schemaname = 'public'
    AND tablename LIKE 'txc_%'
    ORDER BY tablename
""")

print("\nTable sizes:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

print("\n✓ Data loaded successfully!")

cur.close()
conn.close()
