"""
Check if TransXChange data already exists in database
"""

import psycopg2
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

print("Checking TransXChange tables...")
print("="*80)

# Check if tables exist
cur.execute("""
    SELECT tablename 
    FROM pg_tables 
    WHERE schemaname = 'public' 
    AND tablename LIKE 'txc_%'
    ORDER BY tablename
""")

tables = cur.fetchall()
if not tables:
    print("\n❌ No TransXChange tables found")
    print("Run: python scripts/create_transxchange_schema.py")
else:
    print(f"\n✓ Found {len(tables)} tables:")
    for (table,) in tables:
        print(f"  - {table}")

    # Check row counts
    print("\n" + "="*80)
    print("Row counts:")
    print("="*80)
    
    cur.execute("SELECT COUNT(*) FROM txc_stops")
    stops_count = cur.fetchone()[0]
    print(f"txc_stops: {stops_count:,} rows")
    
    cur.execute("SELECT COUNT(*) FROM txc_route_patterns")
    patterns_count = cur.fetchone()[0]
    print(f"txc_route_patterns: {patterns_count:,} rows")
    
    cur.execute("SELECT COUNT(*) FROM txc_pattern_stops")
    sequences_count = cur.fetchone()[0]
    print(f"txc_pattern_stops: {sequences_count:,} rows")
    
    # Check table sizes
    print("\n" + "="*80)
    print("Table sizes:")
    print("="*80)
    
    cur.execute("""
        SELECT 
            tablename,
            pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename LIKE 'txc_%'
        ORDER BY pg_total_relation_size('public.'||tablename) DESC
    """)
    
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]}")
    
    # Sample data
    if stops_count > 0:
        print("\n" + "="*80)
        print("Sample data (first 3 stops):")
        print("="*80)
        
        cur.execute("""
            SELECT naptan_id, stop_name, latitude, longitude 
            FROM txc_stops 
            LIMIT 3
        """)
        
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} ({row[2]:.5f}, {row[3]:.5f})")
    
    if patterns_count > 0:
        print("\nSample routes (first 3):")
        cur.execute("""
            SELECT service_code, operator_name, route_name, direction 
            FROM txc_route_patterns 
            LIMIT 3
        """)
        
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} - Route {row[2]} ({row[3]})")

cur.close()
conn.close()
