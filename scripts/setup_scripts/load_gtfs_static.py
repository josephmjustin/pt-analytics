import csv
import psycopg2
from psycopg2.extras import execute_values
import sys

def load_routes(cursor):
    routes = []
    
    with open("../static/routes.txt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            route_id = row.get("route_id") or None
            agency_id = row.get("agency_id") or None
            route_short_name = row.get("route_short_name") or None
            route_long_name = row.get("route_long_name") or None
            route_type = row.get("route_type") or None
            routes.append((route_id, agency_id, route_short_name, route_long_name, route_type)) 

    return routes

def load_trips(cursor):
    trips = []
    
    with open("../static/trips.txt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row.get("trip_id") or None
            route_id = row.get("route_id") or None
            service_id = row.get("service_id") or None 
            trip_headsign = row.get("trip_headsign") or None
            direction_id = row.get("direction_id") or None
            block_id = row.get("block_id") or None
            shape_id = row.get("shape_id") or None
            wheelchair_accessible = row.get("wheelchair_accessible") or None
            vehicle_journey_code = row.get("vehicle_journey_code") or None
            trips.append((trip_id, route_id, service_id, trip_headsign, direction_id, block_id, shape_id, wheelchair_accessible, vehicle_journey_code)) 

    return trips

def load_stop_times(cursor, batch_size=10000):
    print("Starting to read stop_times.txt...", flush=True)
    batch = []
    total = 0
    
    with open("../static/stop_times.txt", encoding="utf-8") as f:
        print("File opened, reading rows...", flush=True)
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row.get("trip_id") or None
            arrival_time = row.get("arrival_time") or None
            departure_time = row.get("departure_time") or None  
            stop_id = row.get("stop_id") or None
            stop_sequence = row.get("stop_sequence") or None
            pickup_type = row.get("pickup_type") or None
            drop_off_type = row.get("drop_off_type") or None
            shape_dist_traveled = row.get("shape_dist_traveled") or None
            timepoint = row.get("timepoint") or None
            
            batch.append((trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, shape_dist_traveled, timepoint))
            
            # Insert when batch is full
            if len(batch) >= batch_size:
                execute_values(cursor, """
                    INSERT INTO gtfs_stop_times 
                    (trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, shape_dist_traveled, timepoint)
                    VALUES %s
                    ON CONFLICT (trip_id, stop_sequence) DO NOTHING
                """, batch)
                total += len(batch)
                print(f"Inserted {total} stop_times...", flush=True)
                batch = []
        
        # Insert remaining rows
        if batch:
            execute_values(cursor, """
                INSERT INTO gtfs_stop_times 
                (trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, shape_dist_traveled, timepoint)
                VALUES %s
                ON CONFLICT (trip_id, stop_sequence) DO NOTHING
            """, batch)
            total += len(batch)
    
    return total

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="pt_analytics_db",
        user="ptqueryer",
        password="pt_pass"
    )
    cursor = conn.cursor()
    
    trips = load_trips(cursor)
    print(f"Loaded {len(trips)} trips into memory, now inserting...", flush=True)
    # Database insert
    execute_values(cursor, """
        INSERT INTO gtfs_trips 
        (trip_id, route_id, service_id, trip_headsign, direction_id, block_id, shape_id, wheelchair_accessible, vehicle_journey_code)
        VALUES %s
        ON CONFLICT (trip_id) DO NOTHING
    """, trips)
    
    conn.commit()
    print(f"Total trips processed: {len(trips)}")

    total_stop_times = load_stop_times(cursor, batch_size=10000)
    conn.commit()
    print(f"Total stop_times processed: {total_stop_times}")

    cursor.close()
    conn.close()
    



except Exception as e:
    print("Error:", e)

finally:
    if conn:
        conn.close()    

