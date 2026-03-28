"""
TransXChange data loader for API
Loads JSON at startup and provides lookup functions
"""

import json
from typing import Dict, List, Optional, Set
import math

# Global variables (loaded on first use)
TXC_DATA: Dict = {}
STOPS: Dict = {}  # naptan_id -> {name, lat, lon}
ROUTE_STOPS: Dict = {}  # route_name -> set of naptan_ids
STOP_ROUTES: Dict = {}  # naptan_id -> list of {route_name, service_code, operator, direction}
_loaded = False  # Flag to track if data is loaded

def ensure_data_loaded():
    """Lazy load data on first use"""
    global _loaded
    if not _loaded:
        load_transxchange_data()
        _loaded = True

def load_transxchange_data(json_path: str = "/data/liverpool_transit_data_enriched.json"):
    """Load TransXChange JSON and build lookup indexes"""
    global TXC_DATA, STOPS, ROUTE_STOPS, STOP_ROUTES
    
    import os
    
    # Check if running locally vs Docker - try multiple paths
    paths_to_try = [
        json_path,  # Docker path: /data/...
        "static/liverpool_transit_data_enriched.json",  # From root
        "../static/liverpool_transit_data_enriched.json",  # From scripts/
        os.path.join(os.path.dirname(__file__), '..', '..', 'static', 'liverpool_transit_data_enriched.json')  # Absolute
    ]
    
    found_path = None
    for path in paths_to_try:
        if os.path.exists(path):
            found_path = path
            break
    
    if not found_path:
        print("ERROR: JSON file not found. Tried:", flush=True)
        for path in paths_to_try:
            print(f"  - {path}", flush=True)
        raise FileNotFoundError("TransXChange data not found")
    
    json_path = found_path
    print(f"Loading TransXChange data from {json_path}...", flush=True)
    
    with open(json_path, 'r', encoding='utf-8') as f:
        TXC_DATA = json.load(f)
    
    # Build stops lookup
    STOPS = TXC_DATA['stops']
    
    # Build route->stops mapping
    for op_name, op_data in TXC_DATA['operators'].items():
        for route in op_data['routes']:
            route_name = route['route_name']
            service_code = route['service_code']
            direction = route.get('direction', 'unknown')
            
            # Initialize route
            if route_name not in ROUTE_STOPS:
                ROUTE_STOPS[route_name] = set()
            
            # Add all stops for this route
            for naptan_id in route['stops']:
                ROUTE_STOPS[route_name].add(naptan_id)
                
                # Build reverse lookup: stop -> routes
                if naptan_id not in STOP_ROUTES:
                    STOP_ROUTES[naptan_id] = []
                
                STOP_ROUTES[naptan_id].append({
                    'route_name': route_name,
                    'service_code': service_code,
                    'operator': op_name,
                    'direction': direction,
                    'destination': route.get('description')
                })
    
    print(f"✓ Loaded {len(STOPS):,} stops", flush=True)
    print(f"✓ Loaded {len(ROUTE_STOPS):,} routes", flush=True)
    print("✓ Built lookup indexes", flush=True)

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in meters between two lat/lon points"""
    R = 6371000  # Earth radius in meters
    
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def find_nearest_stop(lat: float, lon: float, route_name: Optional[str] = None, radius_m: float = 10) -> Optional[tuple]:
    """
    Find nearest stop within radius
    If route_name provided, only search stops that route serves
    
    Returns: (naptan_id, stop_data, distance_m) or None
    """
    ensure_data_loaded()  # Lazy load data
    
    # Get valid stops for this route
    if route_name and route_name in ROUTE_STOPS:
        valid_naptan_ids = ROUTE_STOPS[route_name]
    else:
        valid_naptan_ids = STOPS.keys()
    
    nearest = None
    min_distance = radius_m
    
    for naptan_id in valid_naptan_ids:
        stop = STOPS.get(naptan_id)
        if not stop or stop['lat'] is None or stop['lon'] is None:
            continue
        
        distance = haversine(lat, lon, stop['lat'], stop['lon'])
        
        if distance <= min_distance:
            min_distance = distance
            nearest = (naptan_id, stop, distance)
    
    return nearest

def get_routes_at_stop(naptan_id: str) -> List[Dict]:
    """Get all routes serving a stop"""
    ensure_data_loaded()  # Lazy load data
    return STOP_ROUTES.get(naptan_id, [])

def does_route_serve_stop(route_name: str, naptan_id: str) -> bool:
    """Check if a route serves a specific stop"""
    ensure_data_loaded()  # Lazy load data
    return route_name in ROUTE_STOPS and naptan_id in ROUTE_STOPS[route_name]

def get_stop_info(naptan_id: str) -> Optional[Dict]:
    """Get stop information"""
    ensure_data_loaded()  # Lazy load data
    return STOPS.get(naptan_id)

def get_all_stops_for_route(route_name: str) -> Set[str]:
    """Get all stop NaPTAN IDs for a route"""
    ensure_data_loaded()  # Lazy load data
    return ROUTE_STOPS.get(route_name, set())
