"""
Parse TransXChange files and create efficient structure with stops database
Separates stops (with lat/lon) from route patterns to avoid duplication
OPTIMIZED: Multiprocessing for 9600+ files
"""

import os
import xml.etree.ElementTree as ET
import json
from pathlib import Path
from collections import defaultdict
from multiprocessing import Pool, cpu_count

# TransXChange namespace
NS = {'txc': 'http://www.transxchange.org.uk/'}

def parse_operator_info(root):
    """Extract operator information from XML"""
    operators = {}
    for operator in root.findall('.//txc:Operators/txc:Operator', NS):
        op_id = operator.get('id')
        noc = operator.find('txc:NationalOperatorCode', NS)
        short_name = operator.find('txc:OperatorShortName', NS)
        full_name = operator.find('txc:OperatorNameOnLicence', NS)
        
        if op_id and short_name is not None:
            operators[op_id] = {
                'noc': noc.text if noc is not None else None,
                'short_name': short_name.text,
                'full_name': full_name.text if full_name is not None else short_name.text
            }
    
    return operators

def is_liverpool_stop(naptan_id):
    """Check if stop is in Liverpool area (NaPTAN prefix 2800)"""
    return naptan_id.startswith('2800')

def parse_file_wrapper(xml_path):
    """
    Wrapper for multiprocessing - each process has its own stops dict
    Returns: (stops_dict, routes_list)
    """
    local_stops = {}
    routes = parse_transxchange_file(xml_path, local_stops)
    return (local_stops, routes)

def parse_transxchange_file(xml_path, global_stops):
    """
    Extract operator, routes, and stops from TransXChange XML.
    Updates global_stops dict with stop info including lat/lon
    Returns: dict with operator and route info (referencing stop IDs only)
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Get operator info
        operators = parse_operator_info(root)
        
        # Build stop info with lat/lon from AnnotatedStopPointRef
        for annotated_stop in root.findall('.//txc:StopPoints/txc:AnnotatedStopPointRef', NS):
            ref = annotated_stop.find('txc:StopPointRef', NS)
            if ref is None:
                continue
            
            naptan = ref.text
            if not is_liverpool_stop(naptan):
                continue
            
            # Only add if not already in global stops
            if naptan not in global_stops:
                name = annotated_stop.find('txc:CommonName', NS)
                location = annotated_stop.find('txc:Location', NS)
                
                stop_data = {
                    'name': name.text if name is not None else naptan,
                    'lat': None,
                    'lon': None
                }
                
                if location is not None:
                    lat_elem = location.find('txc:Latitude', NS)
                    lon_elem = location.find('txc:Longitude', NS)
                    
                    if lat_elem is not None:
                        stop_data['lat'] = float(lat_elem.text)
                    if lon_elem is not None:
                        stop_data['lon'] = float(lon_elem.text)
                
                global_stops[naptan] = stop_data
        
        routes_data = []
        
        # Get all services
        for service in root.findall('.//txc:Services/txc:Service', NS):
            service_code = service.find('txc:ServiceCode', NS)
            if service_code is None:
                continue
            
            # Get operator ref
            op_ref_elem = service.find('.//txc:RegisteredOperatorRef', NS)
            op_ref = op_ref_elem.text if op_ref_elem is not None else '1'
            operator_info = operators.get(op_ref, {'short_name': 'Unknown', 'noc': None, 'full_name': 'Unknown'})
            
            # Get route/line information
            lines = service.findall('.//txc:Lines/txc:Line', NS)
            for line in lines:
                line_name_elem = line.find('txc:LineName', NS)
                if line_name_elem is None:
                    continue
                route_name = line_name_elem.text
                
                # Get description
                outbound_desc = line.find('txc:OutboundDescription/txc:Description', NS)
                inbound_desc = line.find('txc:InboundDescription/txc:Description', NS)
                
                # Get StandardService
                std_service = service.find('.//txc:StandardService', NS)
                if std_service is None:
                    continue
                
                # Get journey patterns
                journey_patterns = std_service.findall('.//txc:JourneyPattern', NS)
                
                for pattern in journey_patterns:
                    pattern_id = pattern.get('id', 'unknown')
                    direction_elem = pattern.find('txc:Direction', NS)
                    direction = direction_elem.text if direction_elem is not None else "unknown"
                    
                    # Get JourneyPatternSectionRefs
                    jps_refs = pattern.findall('.//txc:JourneyPatternSectionRefs', NS)
                    
                    stop_sequence = []
                    
                    for jps_ref in jps_refs:
                        ref = jps_ref.text
                        if ref:
                            jps = root.find(f'.//txc:JourneyPatternSections/txc:JourneyPatternSection[@id="{ref}"]', NS)
                            if jps is not None:
                                stops_dict = {}
                                
                                for link in jps.findall('.//txc:JourneyPatternTimingLink', NS):
                                    for elem in [link.find('txc:From', NS), link.find('txc:To', NS)]:
                                        if elem is not None:
                                            seq = elem.get('SequenceNumber')
                                            stop_ref = elem.find('txc:StopPointRef', NS)
                                            if seq and stop_ref is not None:
                                                naptan = stop_ref.text
                                                if is_liverpool_stop(naptan):
                                                    stops_dict[int(seq)] = naptan
                                
                                # Add stops in sequence
                                for seq in sorted(stops_dict.keys()):
                                    stop_sequence.append(stops_dict[seq])
                    
                    # Only include routes that have Liverpool stops
                    if stop_sequence:
                        route_desc = None
                        origin_stop = stop_sequence[0] if stop_sequence else None
                        destination_stop = stop_sequence[-1] if stop_sequence else None
                        
                        if direction == 'outbound' and outbound_desc is not None:
                            route_desc = outbound_desc.text
                        elif direction == 'inbound' and inbound_desc is not None:
                            route_desc = inbound_desc.text
                        
                        routes_data.append({
                            'operator': operator_info,
                            'route_name': route_name,
                            'service_code': service_code.text,
                            'direction': direction,
                            'description': route_desc,
                            'origin': origin_stop,
                            'destination': destination_stop,
                            'stops': stop_sequence  # Just NaPTAN IDs now
                        })
        
        return routes_data
    
    except Exception as e:
        print(f"Error parsing {xml_path}: {e}")
        return []

def process_all_files(input_dir, output_json):
    """
    Process all TransXChange files using multiprocessing
    Separate stops database to avoid duplication
    """
    
    # Step 1: Collect all XML file paths
    print("Collecting XML files...")
    xml_files = []
    for root_dir, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.xml'):
                xml_files.append(os.path.join(root_dir, filename))
    
    total_files = len(xml_files)
    print(f"Found {total_files} XML files")
    
    if total_files == 0:
        print("No XML files found!")
        return
    
    # Step 2: Process files in parallel
    print(f"\nProcessing with {cpu_count()} CPU cores...")
    
    with Pool(cpu_count()) as pool:
        # Use shared dict for stops (not ideal, but works)
        results = pool.map(parse_file_wrapper, xml_files)
    
    # Step 3: Merge results
    print("\nMerging results...")
    global_stops = {}
    operators_data = defaultdict(lambda: {'routes': []})
    
    for stops_dict, routes_list in results:
        # Merge stops
        global_stops.update(stops_dict)
        
        # Merge routes
        for route in routes_list:
            op_name = route['operator']['short_name']
            op_noc = route['operator']['noc']
            
            if not operators_data[op_name].get('noc'):
                operators_data[op_name]['noc'] = op_noc
                operators_data[op_name]['full_name'] = route['operator']['full_name']
            
            operators_data[op_name]['routes'].append({
                'route_name': route['route_name'],
                'service_code': route['service_code'],
                'direction': route['direction'],
                'description': route['description'],
                'origin': route['origin'],
                'destination': route['destination'],
                'stops': route['stops']
            })
    
    # Build final structure
    output = {
        'stops': global_stops,
        'operators': {}
    }
    
    total_routes = 0
    
    for op_name, op_data in operators_data.items():
        output['operators'][op_name] = {
            'noc': op_data['noc'],
            'full_name': op_data['full_name'],
            'routes': op_data['routes']
        }
        total_routes += len(op_data['routes'])
    
    # Write JSON
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    file_size_mb = os.path.getsize(output_json) / (1024 * 1024)
    
    print(f"\nOutput written to: {output_json}")
    print(f"File size: {file_size_mb:.2f} MB")
    print(f"\nSummary:")
    print(f"  Unique stops: {len(global_stops)}")
    print(f"  Operators: {len(output['operators'])}")
    print(f"  Routes: {total_routes}")
    
    # Count stops with coordinates
    stops_with_coords = sum(1 for s in global_stops.values() if s['lat'] is not None)
    print(f"  Stops with lat/lon: {stops_with_coords}/{len(global_stops)}")
    
    # Show operators
    print(f"\nOperators found:")
    for op_name, op_data in sorted(output['operators'].items()):
        route_count = len(op_data['routes'])
        print(f"  {op_name} ({op_data['noc']}): {route_count} routes")

if __name__ == "__main__":
    INPUT_DIR = "C:/Users/justi/Work/Personal/pt-analytics/static/transxchange_downloads"
    OUTPUT_JSON = "C:/Users/justi/Work/Personal/pt-analytics/static/liverpool_transit_data.json"
    
    print("Starting TransXChange parser (Liverpool bbox, efficient structure)...")
    print(f"Input directory: {INPUT_DIR}")
    print(f"Output file: {OUTPUT_JSON}\n")
    
    process_all_files(INPUT_DIR, OUTPUT_JSON)
    
    print("\nDone!")
