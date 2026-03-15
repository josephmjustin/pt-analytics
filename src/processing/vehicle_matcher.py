"""
Vehicle Matcher - OPTIMIZED with PostGIS spatial queries
10-100x faster than Python haversine loops
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.api.database_sync import get_db_connection


def find_nearest_stop_for_route_postgis(lat: float, lon: float, route_name: str, direction: str = None, radius_m: float = 30.0):
    """
    Find nearest stop using PostGIS spatial index
    MUCH faster than haversine loop - uses native database spatial queries
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Build query with direction filter if provided
        if direction:
            query = """
                SELECT DISTINCT
                    s.naptan_id,
                    s.stop_name,
                    ST_Distance(
                        s.geog,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ) as distance
                FROM txc_stops s
                JOIN txc_pattern_stops ps ON s.naptan_id = ps.naptan_id
                JOIN txc_route_patterns rp ON ps.pattern_id = rp.pattern_id
                WHERE rp.route_name = %s 
                  AND rp.direction = %s
                  AND ST_DWithin(
                      s.geog,
                      ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                      %s
                  )
                ORDER BY distance
                LIMIT 1
            """
            cur.execute(query, (lon, lat, route_name, direction, lon, lat, radius_m))
        else:
            # Fallback: no direction filter
            query = """
                SELECT DISTINCT
                    s.naptan_id,
                    s.stop_name,
                    ST_Distance(
                        s.geog,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ) as distance
                FROM txc_stops s
                JOIN txc_pattern_stops ps ON s.naptan_id = ps.naptan_id
                JOIN txc_route_patterns rp ON ps.pattern_id = rp.pattern_id
                WHERE rp.route_name = %s
                  AND ST_DWithin(
                      s.geog,
                      ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                      %s
                  )
                ORDER BY distance
                LIMIT 1
            """
            cur.execute(query, (lon, lat, route_name, lon, lat, radius_m))
        
        result = cur.fetchone()
        
        if result:
            return (result['naptan_id'], result['stop_name'], result['distance'])
        return None
        
    finally:
        cur.close()
        conn.close()


def match_vehicle_to_stop(vehicle_position: dict) -> dict:
    """Match vehicle to nearest valid stop using PostGIS spatial queries"""
    
    vehicle_id = vehicle_position['vehicle_id']
    lat = vehicle_position['latitude']
    lon = vehicle_position['longitude']
    timestamp = vehicle_position.get('timestamp') or vehicle_position.get('stop_timestamp')
    route_name = vehicle_position.get('route_name')
    direction = vehicle_position.get('direction')
    
    if not route_name:
        return {
            'vehicle_id': vehicle_id,
            'route_name': None,
            'direction': direction,
            'naptan_id': None,
            'timestamp': timestamp,
            'matched': False
        }
    
    # Use PostGIS spatial query - MUCH faster!
    nearest = find_nearest_stop_for_route_postgis(lat, lon, route_name, direction, radius_m=30.0)
    
    if not nearest:
        return {
            'vehicle_id': vehicle_id,
            'route_name': route_name,
            'direction': direction,
            'naptan_id': None,
            'timestamp': timestamp,
            'matched': False
        }
    
    naptan_id, stop_name, distance = nearest
    
    return {
        'vehicle_id': vehicle_id,
        'route_name': route_name,
        'direction': direction,
        'naptan_id': naptan_id,
        'stop_name': stop_name,
        'distance_m': round(distance, 1),
        'timestamp': timestamp,
        'matched': True
    }