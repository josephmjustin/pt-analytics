"""
Optimized BODS SIRI-VM polling and ingestion
Fetches vehicle positions every 10 seconds with direction and route info
"""
import sys
import os
import requests
import xml.etree.ElementTree as ET
import logging
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Add project paths for direct execution. Redundant if run via Celery task, but allows `python scripts/run_analysis.py` to work without issues.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.api.database_sync import get_db_connection

load_dotenv()
logger = logging.getLogger(__name__)

# BODS API Configuration
BODS_API_KEY = os.getenv("BODS_API_KEY")

if not BODS_API_KEY:
    raise ValueError("BODS_API_KEY not set in environment variables")

LIVERPOOL_BBOX = "-3.05,53.35,-2.85,53.48"
SIRI_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/?api_key={BODS_API_KEY}&boundingBox={LIVERPOOL_BBOX}"


# SIRI namespace
NS = {
    'siri': 'http://www.siri.org.uk/siri'
}


def fetch_vehicle_positions():
    """Fetch vehicle positions from BODS SIRI-VM API"""
    try:
        response = requests.get(SIRI_URL, timeout=30)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        vehicle_activities = root.findall('.//siri:VehicleActivity', NS)
        
        vehicles = []
        now = datetime.now(tz=None)  # Current time for age comparison
        max_age = timedelta(minutes=5)  # Skip positions older than 5 minutes
        
        for activity in vehicle_activities:
            mvj = activity.find('.//siri:MonitoredVehicleJourney', NS)
            
            if mvj is None:
                continue
            
            # Extract location
            vehicle_location = mvj.find('siri:VehicleLocation', NS)
            if vehicle_location is None:
                continue
            
            longitude = vehicle_location.find('siri:Longitude', NS)
            latitude = vehicle_location.find('siri:Latitude', NS)
            
            if longitude is None or latitude is None:
                continue
            
            # Extract all available fields
            vehicle_ref = mvj.find('siri:VehicleRef', NS)
            line_ref = mvj.find('siri:LineRef', NS)
            direction_ref = mvj.find('siri:DirectionRef', NS)
            operator_ref = mvj.find('siri:OperatorRef', NS)
            origin_name = mvj.find('siri:OriginName', NS)
            destination_name = mvj.find('siri:DestinationName', NS)
            bearing = mvj.find('siri:Bearing', NS)
            recorded_at = activity.find('.//siri:RecordedAtTime', NS)
            journey_ref = mvj.find('siri:FramedVehicleJourneyRef/siri:DatedVehicleJourneyRef', NS)
            
            # Parse timestamp
            if recorded_at is not None and recorded_at.text is not None:
                timestamp = datetime.fromisoformat(recorded_at.text.replace('Z', '+00:00'))
                # Make timezone-naive for comparison
                timestamp_naive = timestamp.replace(tzinfo=None)
            else:
                timestamp = datetime.now()
                timestamp_naive = timestamp
            
            # Skip old positions
            position_age = now - timestamp_naive
            if position_age > max_age:
                continue
            
            if latitude.text is None or longitude.text is None:
                continue

            vehicle_data = {
                'vehicle_id': vehicle_ref.text if vehicle_ref is not None else None,
                'route_name': line_ref.text if line_ref is not None else None,
                'direction': direction_ref.text if direction_ref is not None else None,
                'operator': operator_ref.text if operator_ref is not None else None,
                'origin': origin_name.text if origin_name is not None else None,
                'destination': destination_name.text if destination_name is not None else None,
                'latitude': float(latitude.text),
                'longitude': float(longitude.text),
                'bearing': float(bearing.text) if bearing is not None and bearing.text is not None else None,
                'timestamp': timestamp,
                'trip_id': journey_ref.text if journey_ref is not None else None
            }
            
            vehicles.append(vehicle_data)
        
        return vehicles
    
    except Exception as e:
        logger.error(f"Error fetching SIRI-VM: {e}", exc_info=True)
        raise


def store_vehicle_positions(vehicles):
    """Store vehicle positions with direction and route info"""
    if not vehicles:
        return 0
    
    conn = None
    cur = None
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        values = [
            (
                v['vehicle_id'],
                v['latitude'],
                v['longitude'],
                v['timestamp'],
                v['route_name'],
                v['trip_id'],
                v['bearing'],
                v['direction'],
                v['operator'],
                v['origin'],
                v['destination']
            )
            for v in vehicles
        ]
        
        execute_batch(cur, """
            INSERT INTO vehicle_positions
            (vehicle_id, latitude, longitude, timestamp, route_name, trip_id, bearing,
            direction, operator, origin, destination)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vehicle_id, timestamp) DO UPDATE SET
                route_name = EXCLUDED.route_name,
                direction = EXCLUDED.direction,
                operator = EXCLUDED.operator,
                origin = EXCLUDED.origin,
                destination = EXCLUDED.destination,
                analyzed = false
        """, values, page_size=500)
        
        conn.commit()
        return len(vehicles)
    
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error storing positions: {e}", exc_info=True)
        raise
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def poll_and_ingest():
    """Main polling function - called by Celery every 10 seconds"""
    
    # Fetch vehicle positions
    vehicles = fetch_vehicle_positions()
    
    if not vehicles:
        logger.info(f"No vehicles fetched")
        return {"stored": 0, "total": 0, "with_direction": 0}
    
    # Count vehicles with direction
    with_direction = sum(1 for v in vehicles if v['direction'] is not None)
    
    # Store positions
    stored = store_vehicle_positions(vehicles)
    result = {"stored": stored, "total": len(vehicles), "with_direction": with_direction}
    logger.info(f"Stored {stored}/{len(vehicles)} positions ({with_direction} with direction)")
    return result

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    poll_and_ingest()