"""
Map operator codes (A2BV, SCMY) to readable names (Arriva, Stagecoach)
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# UK Bus Operator Code to Name mapping
# Source: BODS NOC (National Operator Codes)
OPERATOR_CODE_MAP = {
    'A2BV': 'Arriva',
    'AMSY': 'Arriva',
    'ANWE': 'Arriva',
    'SCMY': 'Stagecoach',
    'SCMR': 'Stagecoach', 
    'FECS': 'First Bus',
    'FESX': 'First Bus',
    'NATX': 'National Express',
    'HATT': 'Hattons',
    'HUYT': 'Huyton Travel',
    'HTL': 'Huyton Travel',
    'BPTR': 'Beaver Travel',
    'LCUT': 'Cumfybus',
    'MCSL': 'M-CABS',
    'CBBH': 'Cumfybus',
    'GONW': 'Go North West',
    'DFDS': 'Warrington\'s Own Buses',
}

def check_current_operators():
    """Check what operator codes we have"""
    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    cur = conn.cursor()
    
    print("="*80)
    print("CURRENT OPERATORS IN DATABASE")
    print("="*80)
    
    # vehicle_arrivals
    print("\nOperators in vehicle_arrivals:")
    cur.execute("""
        SELECT 
            operator,
            COUNT(*) as arrivals,
            COUNT(DISTINCT route_name) as routes
        FROM vehicle_arrivals
        GROUP BY operator
        ORDER BY arrivals DESC
    """)
    
    for row in cur.fetchall():
        code = row['operator']
        name = OPERATOR_CODE_MAP.get(code, '???')
        print(f"  {code:10} → {name:20} | {row['arrivals']:5} arrivals, {row['routes']} routes")
    
    # vehicle_positions
    print("\nOperators in vehicle_positions:")
    cur.execute("""
        SELECT 
            operator,
            COUNT(*) as positions,
            COUNT(DISTINCT route_name) as routes
        FROM vehicle_positions
        WHERE operator IS NOT NULL
        GROUP BY operator
        ORDER BY positions DESC
        LIMIT 10
    """)
    
    for row in cur.fetchall():
        code = row['operator']
        name = OPERATOR_CODE_MAP.get(code, '???')
        print(f"  {code:10} → {name:20} | {row['positions']:5} positions, {row['routes']} routes")
    
    cur.close()
    conn.close()
    
    print("\n" + "="*80)

def map_operator_codes_to_names():
    """Replace operator codes with readable names"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("\n" + "="*80)
    print("MAPPING OPERATOR CODES TO NAMES")
    print("="*80)
    
    updated_total = 0
    
    for code, name in OPERATOR_CODE_MAP.items():
        # Update vehicle_arrivals
        cur.execute("""
            UPDATE vehicle_arrivals
            SET operator = %s
            WHERE operator = %s
        """, (name, code))
        
        updated = cur.rowcount
        if updated > 0:
            print(f"✓ {code:10} → {name:20} ({updated} records)")
            updated_total += updated
    
    conn.commit()
    
    print(f"\n✓ Total updated: {updated_total} records")
    
    # Show final distribution
    print("\nFinal operator distribution:")
    cur.execute("""
        SELECT 
            operator,
            COUNT(*) as arrivals,
            COUNT(DISTINCT route_name) as routes
        FROM vehicle_arrivals
        GROUP BY operator
        ORDER BY arrivals DESC
    """)
    
    for row in cur.fetchall():
        print(f"  {row[0]:30} {row[1]:5} arrivals, {row[2]} routes")
    
    cur.close()
    conn.close()
    
    print("\n" + "="*80)

def fix_remaining_unknown():
    """Fix remaining Unknown operators using multiple strategies"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("\n" + "="*80)
    print("FIXING REMAINING 'UNKNOWN' OPERATORS")
    print("="*80)
    
    # Strategy 1: Map from vehicle_positions by vehicle_id + similar timestamp
    print("\n1. Mapping from vehicle_positions (by vehicle_id + timestamp)...")
    cur.execute("""
        WITH position_operators AS (
            SELECT DISTINCT
                vehicle_id,
                operator,
                timestamp
            FROM vehicle_positions
            WHERE operator IS NOT NULL
              AND operator != ''
        )
        UPDATE vehicle_arrivals va
        SET operator = po.operator
        FROM position_operators po
        WHERE va.vehicle_id = po.vehicle_id
          AND ABS(EXTRACT(EPOCH FROM (va.timestamp - po.timestamp))) < 120
          AND va.operator = 'Unknown'
    """)
    
    updated = cur.rowcount
    print(f"   ✓ Updated {updated} records from vehicle_positions")
    conn.commit()
    
    # Strategy 2: Map from txc_route_patterns
    print("\n2. Mapping from TransXChange route data...")
    cur.execute("""
        WITH route_operators AS (
            SELECT DISTINCT
                route_name,
                direction,
                operator_name
            FROM txc_route_patterns
        )
        UPDATE vehicle_arrivals va
        SET operator = ro.operator_name
        FROM route_operators ro
        WHERE va.route_name = ro.route_name
          AND (va.direction = ro.direction OR va.direction IS NULL OR ro.direction IS NULL)
          AND va.operator = 'Unknown'
    """)
    
    updated = cur.rowcount
    print(f"   ✓ Updated {updated} records from TransXChange")
    conn.commit()
    
    # Check what's left
    cur.execute("""
        SELECT COUNT(*) as still_unknown
        FROM vehicle_arrivals
        WHERE operator = 'Unknown'
    """)
    
    still_unknown = cur.fetchone()[0]
    
    if still_unknown > 0:
        print(f"\n⚠ Still have {still_unknown} Unknown operators")
        
        # Show sample
        cur.execute("""
            SELECT DISTINCT
                route_name,
                direction,
                COUNT(*) as count
            FROM vehicle_arrivals
            WHERE operator = 'Unknown'
            GROUP BY route_name, direction
            ORDER BY count DESC
            LIMIT 10
        """)
        
        print("\n   Routes still Unknown:")
        for row in cur.fetchall():
            print(f"      {row[0]:10} {row[1] or 'NULL':10} {row[2]} arrivals")
    else:
        print("\n✓ All operators successfully mapped!")
    
    cur.close()
    conn.close()
    
    print("\n" + "="*80)

def create_operator_mapping_table():
    """Create operator code mapping table for future use"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("\n" + "="*80)
    print("CREATING OPERATOR MAPPING TABLE")
    print("="*80)
    
    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS operator_codes (
            operator_code VARCHAR(10) PRIMARY KEY,
            operator_name VARCHAR(100) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    
    # Insert mappings
    for code, name in OPERATOR_CODE_MAP.items():
        cur.execute("""
            INSERT INTO operator_codes (operator_code, operator_name)
            VALUES (%s, %s)
            ON CONFLICT (operator_code) 
            DO UPDATE SET operator_name = EXCLUDED.operator_name
        """, (code, name))
    
    conn.commit()
    
    print(f"✓ Created operator_codes table with {len(OPERATOR_CODE_MAP)} mappings")
    
    # Create view for easy lookup
    cur.execute("""
        CREATE OR REPLACE VIEW v_arrivals_with_operators AS
        SELECT 
            va.*,
            COALESCE(oc.operator_name, va.operator) as operator_full_name
        FROM vehicle_arrivals va
        LEFT JOIN operator_codes oc ON va.operator = oc.operator_code;
    """)
    
    print("✓ Created v_arrivals_with_operators view")
    
    cur.close()
    conn.close()
    
    print("\n" + "="*80)

if __name__ == "__main__":
    print("PT Analytics - Operator Mapping Tool")
    print("="*80)
    
    # Step 1: Show current state
    check_current_operators()
    
    # Step 2: Ask user
    print("\nActions available:")
    print("1. Map operator codes to names (A2BV → Arriva)")
    print("2. Fix remaining Unknown operators")
    print("3. Create operator mapping table")
    print("4. Do all of the above")
    print("\nChoice (1-4): ", end='')
    
    choice = input().strip()
    
    if choice == '1':
        map_operator_codes_to_names()
    elif choice == '2':
        fix_remaining_unknown()
    elif choice == '3':
        create_operator_mapping_table()
    elif choice == '4':
        map_operator_codes_to_names()
        fix_remaining_unknown()
        create_operator_mapping_table()
        
        print("\n✓ All operations complete!")
        print("\nNext steps:")
        print("1. Check data: SELECT operator, COUNT(*) FROM vehicle_arrivals GROUP BY operator;")
        print("2. Run SRI pipeline: python scripts/aggregate_headway_patterns.py")
        print("3. Test API: http://localhost:8000/sri/network")
    else:
        print("Invalid choice")