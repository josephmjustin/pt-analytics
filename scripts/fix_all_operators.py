#!/usr/bin/env python3
"""
Verify and fix ALL operator issues across all tables
Run this on Oracle VM
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.database_sync import get_db_connection
from psycopg2.extras import RealDictCursor

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
}

def check_all_tables():
    """Check operator status in all tables"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("="*80)
    print("CHECKING OPERATORS IN ALL TABLES")
    print("="*80)
    
    # 1. vehicle_positions
    print("\n1. vehicle_positions:")
    cur.execute("""
        SELECT operator, COUNT(*) as count
        FROM vehicle_positions
        WHERE operator IS NOT NULL
        GROUP BY operator
        ORDER BY count DESC
        LIMIT 10
    """)
    for row in cur.fetchall():
        mapped = OPERATOR_CODE_MAP.get(row['operator'], row['operator'])
        symbol = "→" if row['operator'] in OPERATOR_CODE_MAP else " "
        print(f"   {row['operator']:10} {symbol:2} {mapped:20} {row['count']:,} records")
    
    # 2. vehicle_arrivals
    print("\n2. vehicle_arrivals:")
    cur.execute("""
        SELECT operator, COUNT(*) as count
        FROM vehicle_arrivals
        GROUP BY operator
        ORDER BY count DESC
    """)
    for row in cur.fetchall():
        symbol = "✓" if row['operator'] not in ['Unknown', 'A2BV', 'SCMY', 'AMSY'] else "✗"
        print(f"   {symbol} {row['operator']:20} {row['count']:,} records")
    
    # 3. schedule_adherence_patterns
    print("\n3. schedule_adherence_patterns:")
    try:
        cur.execute("""
            SELECT operator, COUNT(*) as count
            FROM schedule_adherence_patterns
            GROUP BY operator
            ORDER BY count DESC
            LIMIT 10
        """)
        for row in cur.fetchall():
            symbol = "✓" if row['operator'] not in ['Unknown', 'A2BV', 'SCMY'] else "✗"
            print(f"   {symbol} {row['operator']:20} {row['count']:,} records")
    except:
        print("   (table empty or doesn't exist)")
    
    cur.close()
    conn.close()
    
    print("\n" + "="*80)

def fix_all_operators():
    """Fix operators in ALL tables"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("\n" + "="*80)
    print("FIXING OPERATORS IN ALL TABLES")
    print("="*80)
    
    total_fixed = 0
    
    # 1. Fix vehicle_arrivals
    print("\n1. Fixing vehicle_arrivals...")
    
    # Map codes to names
    for code, name in OPERATOR_CODE_MAP.items():
        cur.execute("""
            UPDATE vehicle_arrivals
            SET operator = %s
            WHERE operator = %s
        """, (name, code))
        
        if cur.rowcount > 0:
            print(f"   {code:10} → {name:20} ({cur.rowcount} records)")
            total_fixed += cur.rowcount
    
    # Fix Unknown from vehicle_positions
    cur.execute("""
        UPDATE vehicle_arrivals va
        SET operator = CASE 
            WHEN vp.operator IN ('A2BV', 'AMSY', 'ANWE') THEN 'Arriva'
            WHEN vp.operator IN ('SCMY', 'SCMR') THEN 'Stagecoach'
            WHEN vp.operator IN ('FECS', 'FESX') THEN 'First Bus'
            WHEN vp.operator = 'NATX' THEN 'National Express'
            WHEN vp.operator = 'HATT' THEN 'Hattons'
            WHEN vp.operator IN ('HUYT', 'HTL') THEN 'Huyton Travel'
            ELSE vp.operator
        END
        FROM vehicle_positions vp
        WHERE va.vehicle_id = vp.vehicle_id
          AND ABS(EXTRACT(EPOCH FROM (va.timestamp - vp.timestamp))) < 120
          AND va.operator = 'Unknown'
          AND vp.operator IS NOT NULL
    """)
    
    if cur.rowcount > 0:
        print(f"   Unknown → mapped      ({cur.rowcount} records)")
        total_fixed += cur.rowcount
    
    conn.commit()
    print(f"   ✓ Fixed {total_fixed} records in vehicle_arrivals")
    
    # 2. Clean schedule_adherence_patterns (will be regenerated)
    print("\n2. Cleaning schedule_adherence_patterns...")
    try:
        cur.execute("DELETE FROM schedule_adherence_patterns WHERE operator IN ('Unknown', 'A2BV', 'SCMY', 'AMSY', 'ANWE', 'SCMR')")
        deleted = cur.rowcount
        conn.commit()
        print(f"   ✓ Deleted {deleted} records with old operator codes")
        print("   (Will regenerate with correct operators on next run)")
    except Exception as e:
        print(f"   ⚠ Could not clean: {e}")
    
    # 3. Clean other SRI tables with operator codes
    sri_tables = [
        'headway_patterns',
        'journey_time_patterns',
        'service_delivery_patterns',
        'headway_consistency_scores',
        'schedule_adherence_scores',
        'journey_time_consistency_scores',
        'service_delivery_scores',
        'service_reliability_index',
    ]
    
    print("\n3. Cleaning other SRI tables...")
    for table in sri_tables:
        try:
            cur.execute(f"""
                DELETE FROM {table} 
                WHERE operator IN ('Unknown', 'A2BV', 'SCMY', 'AMSY', 'ANWE', 'SCMR', 'FECS', 'NATX', 'HATT', 'HUYT')
            """)
            if cur.rowcount > 0:
                print(f"   {table:40} deleted {cur.rowcount} records")
        except Exception as e:
            pass  # Table might not exist or have operator column
    
    conn.commit()
    
    cur.close()
    conn.close()
    
    print("\n" + "="*80)
    print("✓ OPERATOR FIX COMPLETE")
    print("="*80)
    print("\nWhat was done:")
    print("1. ✓ Mapped operator codes to names in vehicle_arrivals")
    print("2. ✓ Fixed Unknown operators from vehicle_positions")
    print("3. ✓ Cleaned SRI tables (will regenerate on next analysis)")
    print("\nNext steps:")
    print("1. Run: python cron_scripts/run_analysis.py")
    print("2. SRI tables will regenerate with correct operator names")
    print("3. API will show proper operators (Arriva, Stagecoach, etc.)")

if __name__ == "__main__":
    print("PT ANALYTICS - COMPREHENSIVE OPERATOR FIX")
    print("="*80)
    
    check_all_tables()
    
    print("\nThis will:")
    print("  - Map operator codes to names (A2BV → Arriva)")
    print("  - Fix Unknown operators")
    print("  - Clean and regenerate SRI tables")
    print("\nProceed? (y/n): ", end='')
    
    response = input().strip().lower()
    
    if response == 'y':
        fix_all_operators()
        print("\n" + "="*80)
        print("Verifying fix...")
        check_all_tables()
    else:
        print("\nCancelled.")