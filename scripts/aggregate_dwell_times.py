"""
Aggregate dwell times into analysis table with running averages
"""
import os
import sys
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.api.database_sync import get_db_connection
from psycopg2.extras import execute_batch

def aggregate_dwell_times():
    """Aggregate vehicle_arrivals into dwell_time_analysis table"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dwell_time_analysis (
                naptan_id VARCHAR(20),
                route_name VARCHAR(20),
                direction VARCHAR(20),
                operator VARCHAR(50),
                day_of_week INTEGER,
                hour_of_day INTEGER,
                avg_dwell_seconds REAL,
                stddev_dwell_seconds REAL,
                sample_count INTEGER,
                last_updated TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (naptan_id, route_name, direction, operator, day_of_week, hour_of_day)
            );
            
            CREATE INDEX IF NOT EXISTS idx_dta_route_stop 
            ON dwell_time_analysis(route_name, naptan_id);
            
            CREATE INDEX IF NOT EXISTS idx_dta_high_demand 
            ON dwell_time_analysis(avg_dwell_seconds DESC) 
            WHERE sample_count > 10;
        """)
        
        # Aggregate new arrivals
        cur.execute("""
            INSERT INTO dwell_time_analysis 
            (naptan_id, route_name, direction, operator, day_of_week, hour_of_day, 
             avg_dwell_seconds, stddev_dwell_seconds, sample_count, last_updated)
            SELECT 
                naptan_id,
                route_name,
                direction,
                operator,
                EXTRACT(DOW FROM timestamp)::INTEGER AS day_of_week,
                EXTRACT(HOUR FROM timestamp)::INTEGER AS hour_of_day,
                AVG(dwell_time_seconds)::REAL AS avg_dwell_seconds,
                STDDEV(dwell_time_seconds)::REAL AS stddev_dwell_seconds,
                COUNT(*)::INTEGER AS sample_count,
                NOW() AS last_updated
            FROM vehicle_arrivals
            WHERE dwell_time_seconds IS NOT NULL
            GROUP BY naptan_id, route_name, direction, operator, day_of_week, hour_of_day
            ON CONFLICT (naptan_id, route_name, direction, operator, day_of_week, hour_of_day)
            DO UPDATE SET
                avg_dwell_seconds = (
                    dwell_time_analysis.avg_dwell_seconds * dwell_time_analysis.sample_count + 
                    EXCLUDED.avg_dwell_seconds * EXCLUDED.sample_count
                ) / (dwell_time_analysis.sample_count + EXCLUDED.sample_count),
                sample_count = dwell_time_analysis.sample_count + EXCLUDED.sample_count,
                stddev_dwell_seconds = EXCLUDED.stddev_dwell_seconds,
                last_updated = NOW()
        """)
        
        aggregated = cur.rowcount
        
        # Delete processed arrivals
        cur.execute("DELETE FROM vehicle_arrivals WHERE dwell_time_seconds IS NOT NULL")
        deleted = cur.rowcount
        
        conn.commit()
        
        print(f"✓ Aggregated {aggregated} dwell time records")
        print(f"✓ Deleted {deleted} processed arrivals")
        
    except Exception as e:
        print(f"ERROR aggregating dwell times: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print(f"[{datetime.now()}] Starting dwell time aggregation...")
    aggregate_dwell_times()
    print(f"[{datetime.now()}] Complete")