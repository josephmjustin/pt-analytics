"""
Enrich TransXChange data with GTFS Static stop coordinates
Uses stops.txt from GTFS Static data
"""

import json
import csv

# Load the TransXChange JSON
input_file = "C:/Users/justi/Work/Personal/pt-analytics/static/liverpool_transit_data.json"
output_file = "C:/Users/justi/Work/Personal/pt-analytics/static/liverpool_transit_data_enriched.json"
gtfs_stops_file = "C:/Users/justi/Work/Personal/pt-analytics/static/stops.txt"

print("Loading TransXChange JSON...")
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"Total stops in TransXChange: {len(data['stops'])}")

# Count missing coords
missing_coords = sum(1 for s in data['stops'].values() if s['lat'] is None or s['lon'] is None)
print(f"Stops with missing lat/lon: {missing_coords}")

print(f"\nLoading GTFS Static stops.txt...")

# Load GTFS stops
gtfs_stops = {}
with open(gtfs_stops_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        stop_id = row['stop_id']
        gtfs_stops[stop_id] = {
            'name': row['stop_name'],
            'lat': float(row['stop_lat']) if row['stop_lat'] else None,
            'lon': float(row['stop_lon']) if row['stop_lon'] else None
        }

print(f"GTFS stops loaded: {len(gtfs_stops)}")

# Match TransXChange stops with GTFS stops
enriched = 0
exact_matches = 0
name_matches = 0

print("\nMatching stops...")

for naptan_id, stop_data in data['stops'].items():
    if stop_data['lat'] is not None and stop_data['lon'] is not None:
        continue
    
    # Strategy 1: Direct NaPTAN ID match
    if naptan_id in gtfs_stops:
        gtfs_stop = gtfs_stops[naptan_id]
        if gtfs_stop['lat'] is not None:
            stop_data['lat'] = gtfs_stop['lat']
            stop_data['lon'] = gtfs_stop['lon']
            stop_data['match_type'] = 'naptan_id'
            enriched += 1
            exact_matches += 1
            continue
    
    # Strategy 2: Exact name match (case-insensitive)
    stop_name_lower = stop_data['name'].lower().strip()
    for gtfs_id, gtfs_stop in gtfs_stops.items():
        if gtfs_stop['name'].lower().strip() == stop_name_lower and gtfs_stop['lat'] is not None:
            stop_data['lat'] = gtfs_stop['lat']
            stop_data['lon'] = gtfs_stop['lon']
            stop_data['match_type'] = 'name'
            stop_data['matched_gtfs_id'] = gtfs_id
            enriched += 1
            name_matches += 1
            break

print(f"\nEnriched {enriched} stops from GTFS Static")
print(f"  By NaPTAN ID: {exact_matches}")
print(f"  By name: {name_matches}")

# Count remaining missing
still_missing = sum(1 for s in data['stops'].values() if s['lat'] is None or s['lon'] is None)
print(f"Still missing coordinates: {still_missing}")

# Save enriched data
print(f"\nWriting enriched data to: {output_file}")
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

import os
file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
print(f"Output file size: {file_size_mb:.2f} MB")

# Final summary
stops_with_coords = sum(1 for s in data['stops'].values() if s['lat'] is not None and s['lon'] is not None)
coverage_pct = 100 * stops_with_coords / len(data['stops'])

print(f"\nFinal summary:")
print(f"  Total stops: {len(data['stops'])}")
print(f"  Stops with coordinates: {stops_with_coords}")
print(f"  Stops missing coordinates: {len(data['stops']) - stops_with_coords}")
print(f"  Coverage: {coverage_pct:.1f}%")

# Liverpool bbox check
LIVERPOOL_BBOX = {
    'min_lat': 53.35,
    'max_lat': 53.48,
    'min_lon': -3.05,
    'max_lon': -2.85
}

def is_in_bbox(lat, lon):
    if lat is None or lon is None:
        return False
    return (LIVERPOOL_BBOX['min_lat'] <= lat <= LIVERPOOL_BBOX['max_lat'] and
            LIVERPOOL_BBOX['min_lon'] <= lon <= LIVERPOOL_BBOX['max_lon'])

stops_in_bbox = sum(1 for s in data['stops'].values() if is_in_bbox(s['lat'], s['lon']))
print(f"  Stops in Liverpool bbox: {stops_in_bbox} ({100*stops_in_bbox/len(data['stops']):.1f}%)")

print("\nDone!")
