"""
Smart Data Retention with Running Averages
Cleans only tables that actually grow:
- vehicle_positions (grows constantly - clean old analyzed data)
- vehicle_arrivals (grows constantly - clean old arrivals)

SRI tables use running averages - they stay FIXED SIZE and never need cleaning!
"""

import psycopg2
import logging

from src.api.database_sync import get_db_connection

logger = logging.getLogger(__name__)

def cleanup_old_data():
    """
    Clean up tables that grow indefinitely
    SRI tables (service_reliability_index, etc) use running averages - no cleanup needed!
    """
    
    conn = None
    cur = None
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        logger.info("Starting cleanup...")
        
        # ========================================================================
        # 1. VEHICLE POSITIONS - Keep only recent unanalyzed
        # ========================================================================
        logger.info("1. Cleaning vehicle_positions...")
        
        cur.execute("SELECT COUNT(*) FROM vehicle_positions")
        results = cur.fetchone()
        before_positions = results[0] if results else 0
        
        cur.execute("SELECT COUNT(*) FROM vehicle_positions WHERE analyzed = true")
        results = cur.fetchone()
        total_analyzed = results[0] if results else 0
        
        logger.info(f"   Before: {before_positions:,} positions ({total_analyzed:,} analyzed)")
        
        # Count what will be deleted
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE analyzed = true) as analyzed_count,
                COUNT(*) FILTER (WHERE analyzed = false) as unanalyzed_count
            FROM vehicle_positions
            WHERE (analyzed = true AND timestamp < NOW() - INTERVAL '15 minutes')
               OR (analyzed = false AND timestamp < NOW() - INTERVAL '30 minutes')
        """)
        
        result = cur.fetchone()
        analyzed_to_delete = result[0] if result else 0
        unanalyzed_to_delete = result[1] if result else 0
        
        # Delete old data
        cur.execute("""
            DELETE FROM vehicle_positions
            WHERE (analyzed = true AND timestamp < NOW() - INTERVAL '15 minutes')
               OR (analyzed = false AND timestamp < NOW() - INTERVAL '30 minutes')
        """)
        
        total_deleted = cur.rowcount
        conn.commit()
        
        logger.info(f"   Deleted: {analyzed_to_delete:,} analyzed (>15min)")
        if unanalyzed_to_delete > 0:
            logger.warning(f"Deleted {unanalyzed_to_delete:,} unanalyzed (>30min) - analysis falling behind")
            logger.info("      Analysis falling behind!")
        logger.info(f"   After: {before_positions - total_deleted:,} positions")
        
        # ========================================================================
        # 2. VEHICLE ARRIVALS - Delete after aggregation into dwell_time_analysis
        # ========================================================================
        logger.info("2. Cleaning vehicle_arrivals...")
        
        cur.execute("SELECT COUNT(*) FROM vehicle_arrivals")
        results = cur.fetchone()
        old_arrivals = results[0] if results else 0
        
        if old_arrivals > 0:
            # These should already be aggregated by aggregate_dwell_times.py
            # This is a safety cleanup for any stragglers
            cur.execute("DELETE FROM vehicle_arrivals WHERE timestamp < NOW() - INTERVAL '1 hour'")
            deleted = cur.rowcount
            conn.commit()
            logger.info(f"   Deleted: {deleted:,} arrivals (>1 hour)")
        else:
            logger.info("   No old arrivals to clean")
        
        # ========================================================================
        # VACUUM to reclaim disk space
        # ========================================================================
        logger.info("Running VACUUM to reclaim disk space...")
        
        conn.commit()
        cur.close()
        conn.close()
        
        # Reconnect with autocommit for VACUUM
        conn = get_db_connection()
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        cur.execute("VACUUM vehicle_positions")
        cur.execute("VACUUM vehicle_arrivals")
        
        # Check final sizes
        cur.execute("""
            SELECT 
                pg_size_pretty(pg_total_relation_size('vehicle_positions')) as vp_size,
                pg_size_pretty(pg_total_relation_size('vehicle_arrivals')) as va_size
        """)
        sizes = cur.fetchone()
        
        logger.info(f"   vehicle_positions: {sizes[0]}" if sizes and sizes[0] else "unknown")
        logger.info(f"   vehicle_arrivals: {sizes[1]}" if sizes and sizes[1] else "unknown")
        logger.info("✓ Cleanup complete")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cleanup_old_data()
