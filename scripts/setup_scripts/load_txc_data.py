"""
Load enriched TransXChange data into PostgreSQL
Uses pattern_id approach for clean relationships
"""

import json
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT", 5432)
)
cur = conn.cursor()

input_file = "C:/Users/justi/Work/Personal/pt-analytics/static/liverpool_transit_data_enriched.json"

print("Loading JSON...")
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"Loaded: {len(data['stops'])} stops, {len(data['operators'])} operators")

# Count total routes
total_routes = sum(len(op['routes']) for op in data['operators'].values())
print(f"Total route patterns: {total_routes}")

print("\n" + "="*80)
print("LOADING DATA")
print("="*80)

# Step 1: Clear existing data
print("\n1. Clearing existing data...")
cur.execute("TRUNCATE txc_pattern_stops, txc_route_patterns, txc_stops CASCADE")
conn.commit()
print("   ✓ Tables cleared")

# Step 2: Load stops (batch insert)
print("\n2. Loading stops...")
stop_values = [
    (naptan_id, stop['name'], stop['lat'], stop['lon'])
    for naptan_id, stop in data['stops'].items()
    if stop['lat'] is not None and stop['lon'] is not None
]

skipped = len(data['stops']) - len(stop_values)

psycopg2.extras.execute_values(
    cur,
    """
    INSERT INTO txc_stops (naptan_id, stop_name, latitude, longitude)
    VALUES %s
    """,
    stop_values,
    page_size=1000
)
conn.commit()
print(f"   ✓ Loaded {len(stop_values):,} stops")
if skipped > 0:
    print(f"   ⚠ Skipped {skipped} stops without coordinates")

# Step 3: Load route patterns and get pattern_ids back
print("\n3. Loading route patterns...")
pattern_values = []
pattern_metadata = []  # Store (service_code, direction, operator) for later

for op_name, op_data in data['operators'].items():
    for route in op_data['routes']:
        pattern_values.append((
            route['service_code'],
            op_name,
            op_data.get('noc'),
            route['route_name'],
            route.get('direction'),
            route.get('origin'),
            route.get('destination')
        ))
        
        # Store metadata to match with pattern_id later
        pattern_metadata.append({
            'service_code': route['service_code'],
            'direction': route.get('direction'),
            'operator': op_name,
            'stops': route['stops']
        })

# Insert and get pattern_ids back
print(f"   Inserting {len(pattern_values):,} patterns...")

# Use execute_values with RETURNING to get pattern_ids
from io import StringIO
import csv

# Create temp table approach for batch insert with RETURNING
cur.execute("""
    CREATE TEMP TABLE temp_patterns (
        service_code TEXT,
        operator_name TEXT,
        operator_noc TEXT,
        route_name TEXT,
        direction TEXT,
        origin TEXT,
        destination TEXT
    )
""")

# Bulk insert to temp table
psycopg2.extras.execute_values(
    cur,
    """
    INSERT INTO temp_patterns VALUES %s
    """,
    pattern_values,
    page_size=1000
)

# Insert from temp to real table with ON CONFLICT
cur.execute("""
    INSERT INTO txc_route_patterns 
        (service_code, operator_name, operator_noc, route_name, direction, origin, destination)
    SELECT service_code, operator_name, operator_noc, route_name, direction, origin, destination
    FROM temp_patterns
    ON CONFLICT (service_code, direction) DO NOTHING
""")

# Now get ALL pattern_ids (including ones that already existed)
cur.execute("""
    SELECT pattern_id, service_code, direction
    FROM txc_route_patterns
""")

# Build mapping: (service_code, direction) -> pattern_id
pattern_id_map = {}
for row in cur.fetchall():
    pattern_id, service_code, direction = row
    key = f"{service_code}|{direction}"
    pattern_id_map[key] = pattern_id

conn.commit()
print(f"   ✓ Loaded {len(pattern_values):,} route patterns")
print(f"   ✓ Generated pattern_ids 1-{len(pattern_id_map)}")

# Step 4: Load stop sequences using pattern_id
print("\n4. Loading stop sequences...")
sequence_values = []
skipped_stops = set()  # Track stops without coordinates

for meta in pattern_metadata:
    key = f"{meta['service_code']}|{meta['direction']}"
    pattern_id = pattern_id_map.get(key)
    
    if pattern_id:
        for idx, naptan_id in enumerate(meta['stops'], 1):
            # Only add if stop exists in txc_stops (has coordinates)
            if naptan_id in data['stops'] and data['stops'][naptan_id]['lat'] is not None:
                sequence_values.append((pattern_id, naptan_id, idx))
            else:
                skipped_stops.add(naptan_id)

print(f"   Total sequences: {len(sequence_values):,}")
print(f"   Inserting in batches...")

# Insert in chunks
chunk_size = 5000
inserted = 0

for i in range(0, len(sequence_values), chunk_size):
    chunk = sequence_values[i:i+chunk_size]
    
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO txc_pattern_stops (pattern_id, naptan_id, stop_sequence)
        VALUES %s
        ON CONFLICT (pattern_id, stop_sequence) DO NOTHING
        """,
        chunk,
        page_size=1000
    )
    
    inserted += len(chunk)
    if inserted % 50000 == 0:
        print(f"   Progress: {inserted:,}/{len(sequence_values):,}")
    
conn.commit()
print(f"   ✓ Loaded {len(sequence_values):,} stop sequences")
if skipped_stops:
    print(f"   ⚠ Skipped {len(skipped_stops)} sequences for stops without coordinates")

# Verification
print("\n" + "="*80)
print("VERIFICATION")
print("="*80)

cur.execute("SELECT COUNT(*) FROM txc_stops")
stops_count = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM txc_route_patterns")
patterns_count = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM txc_pattern_stops")
sequences_count = cur.fetchone()[0]

print(f"\nStops: {stops_count:,}")
print(f"Route patterns: {patterns_count:,}")
print(f"Stop sequences: {sequences_count:,}")

# Check Route 14
print("\n" + "="*80)
print("ROUTE 14 CHECK")
print("="*80)

cur.execute("""
    SELECT 
        pattern_id,
        operator_name,
        route_name,
        direction,
        service_code
    FROM txc_route_patterns
    WHERE route_name = '14'
    ORDER BY operator_name, direction
""")

route_14s = cur.fetchall()
if route_14s:
    print(f"\nFound {len(route_14s)} Route 14 patterns:")
    for row in route_14s:
        pattern_id, operator, route, direction, service = row
        print(f"  pattern_id={pattern_id}: {route} {direction} ({operator}) - {service}")
        
        # Count stops for this pattern
        cur.execute("SELECT COUNT(*) FROM txc_pattern_stops WHERE pattern_id = %s", (pattern_id,))
        stop_count = cur.fetchone()[0]
        print(f"    → {stop_count} stops in sequence")
else:
    print("\n⚠ Route 14 not found!")

# Table sizes
print("\n" + "="*80)
print("TABLE SIZES")
print("="*80)

cur.execute("""
    SELECT 
        tablename,
        pg_size_pretty(pg_total_relation_size('public.' || tablename))
    FROM pg_tables
    WHERE schemaname = 'public' 
      AND tablename LIKE 'txc_%'
    ORDER BY tablename
""")

for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

cur.close()
conn.close()

print("\n" + "="*80)
print("✓ DATA LOADED SUCCESSFULLY!")
print("="*80)