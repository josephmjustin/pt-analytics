"""
Dwell Time Analysis API Endpoints
Provides demand proxy insights based on dwell time patterns
"""
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from src.api.database import get_db
from pydantic import BaseModel
from typing import Generic, TypeVar, Optional, List

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    limit: int
    offset: int
    next: Optional[str] = None
    prev: Optional[str] = None
    data: List[T]

class DwellTimeStats(BaseModel):
    unique_stops: int
    unique_routes: int
    unique_operators: int
    total_samples: int
    overall_avg_dwell: float
    min_avg_dwell: float
    max_avg_dwell: float

class FilterOptions(BaseModel):
    operators: list[str]
    directions: list[str]

class RouteDwellSummary(BaseModel):
    route_name: str
    stops_with_data: int
    operators: int
    total_samples: int
    avg_dwell: float

class RouteStopDwell(BaseModel):
    naptan_id: str
    stop_name: str
    latitude: float
    longitude: float
    direction: str | None = None
    operator: str | None = None
    day_of_week: int
    hour_of_day: int
    avg_dwell_seconds: float
    stddev_dwell_seconds: float | None = None
    sample_count: int

class DwellPattern(BaseModel):
    route_name: str
    direction: str | None = None
    operator: str
    day_of_week: int
    hour_of_day: int
    avg_dwell_seconds: float
    stddev_dwell_seconds: float | None = None
    sample_count: int

class StopInfo(BaseModel):
    naptan_id: str
    stop_name: str
    latitude: float
    longitude: float

class StopDwellPattern(BaseModel):
    stop: StopInfo
    patterns: list[DwellPattern]
    count: int

class HotspotStops(BaseModel):
    naptan_id: str
    stop_name: str
    latitude: float
    longitude: float
    routes_count: int
    overall_avg_dwell: float
    total_samples: int

class Hotspots(BaseModel):
    hotspots: list[HotspotStops]
    count: int  

class HeatmapData(BaseModel):
    route_name: str
    direction: str | None = None
    operator: str | None = None
    stops: list[str]
    hours: list[int]
    data: list[list[Optional[float]]]

class PaginatedDwellRoutes(PaginatedResponse[RouteDwellSummary]):
    pass

class PaginatedRouteStopDwell(PaginatedResponse[RouteStopDwell]):
    pass

  
router = APIRouter(prefix="/dwell-time", tags=["dwell-time"])

OPERATOR_NAME_MAP = {
    'Arriva Merseyside': 'Arriva',
    'Arriva': 'Arriva',
    'Stagecoach Merseyside': 'Stagecoach', 
    'Stagecoach': 'Stagecoach',
    'First Bus': 'First Bus',
}

@router.get("/stats", response_model=DwellTimeStats)
async def get_dwell_time_stats(conn=Depends(get_db)):
    """Get overall dwell time statistics"""
    
    query = """
        SELECT 
            COUNT(DISTINCT naptan_id) as unique_stops,
            COUNT(DISTINCT route_name) as unique_routes,
            COUNT(DISTINCT operator) as unique_operators,
            SUM(sample_count) as total_samples,
            ROUND(AVG(avg_dwell_seconds)::numeric, 1) as overall_avg_dwell,
            ROUND(MIN(avg_dwell_seconds)::numeric, 1) as min_avg_dwell,
            ROUND(MAX(avg_dwell_seconds)::numeric, 1) as max_avg_dwell
        FROM dwell_time_analysis
    """
    
    stats = await conn.fetchrow(query)
    
    return DwellTimeStats(**dict(stats))

@router.get("/filters", response_model=FilterOptions, include_in_schema=False)
async def get_filter_options(conn=Depends(get_db)):
    """Get available filter options for dropdowns"""
    # Get unique operators
    query_operators = """
        SELECT DISTINCT operator_name
        FROM txc_route_patterns
        ORDER BY operator_name
    """
    operators = [row['operator_name'] for row in await conn.fetch(query_operators)]

    # Get unique directions
    query_directions = """
        SELECT DISTINCT direction
        FROM txc_route_patterns
        WHERE direction IS NOT NULL
        ORDER BY direction
    """
    directions = [row['direction'] for row in await conn.fetch(query_directions)]
    
    return FilterOptions(operators=operators, directions=directions)

@router.get("/routes", response_model=PaginatedDwellRoutes)
async def get_routes_with_dwell_data(
    request: Request,
    search: str | None = Query(
        None,
        description="Search routes by name, case-insensitive"
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_db)
):
    """Get all routes with dwell time data available"""
    params = []
    where = "WHERE route_name IS NOT NULL"
    if search:
        where += " AND LOWER(route_name) LIKE LOWER($" + str(len(params) + 1) + ")"
        params.append(search)

    query = f"""
        SELECT 
            route_name,
            COUNT(DISTINCT naptan_id) as stops_with_data,
            COUNT(DISTINCT operator) as operators,
            SUM(sample_count) as total_samples,
            ROUND(AVG(avg_dwell_seconds)::numeric, 1) as avg_dwell
        FROM dwell_time_analysis
        {where}
        GROUP BY route_name
        ORDER BY route_name
        LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
    """
    params.extend([limit, offset])
    routes = await conn.fetch(query, *params)
    total = await conn.fetchval(f"SELECT COUNT(DISTINCT route_name) FROM dwell_time_analysis {where}", *params[:-2])

    base = str(request.base_url).rstrip("/")
    next_url = f"{base}/dwell-time/routes?limit={limit}&offset={offset + limit}" if offset + limit < total else None
    prev_url = f"{base}/dwell-time/routes?limit={limit}&offset={max(0, offset - limit)}" if offset > 0 else None

    data = [RouteDwellSummary(**dict(route)) for route in routes]

    return PaginatedDwellRoutes(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

@router.get("/route/{route_name}/stops", response_model=PaginatedRouteStopDwell)
async def get_route_stops_dwell(
    route_name: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    direction: str | None = Query(None),
    operator: str | None = Query(None),
    day_of_week: int | None = Query(None, ge=0, le=6),
    hour_of_day: int | None = Query(None, ge=0, le=23),
    conn=Depends(get_db)
):
    """Get dwell time analysis for stops on a route"""
    params = []
    where = "WHERE dta.route_name = $" + str(len(params) + 1)
    params.append(route_name)

    if direction:
        where += " AND direction = $" + str(len(params) + 1)
        params.append(direction)

    if operator:
        where += " AND operator = $" + str(len(params) + 1)
        params.append(operator)
    
    if day_of_week is not None:
        where += " AND day_of_week = $" + str(len(params) + 1)
        params.append(day_of_week)
    
    if hour_of_day is not None:
        where += " AND hour_of_day = $" + str(len(params) + 1)
        params.append(hour_of_day)
    
    total = await conn.fetchval(f"SELECT COUNT(*) FROM dwell_time_analysis dta JOIN txc_stops ts ON dta.naptan_id = ts.naptan_id {where}", *params)

    query = f"""
        SELECT 
            dta.naptan_id,
            ts.stop_name,
            ts.latitude,
            ts.longitude,
            dta.direction,
            dta.operator,
            dta.day_of_week,
            dta.hour_of_day,
            ROUND(dta.avg_dwell_seconds::numeric, 1) as avg_dwell_seconds,
            ROUND(dta.stddev_dwell_seconds::numeric, 1) as stddev_dwell_seconds,
            dta.sample_count
        FROM dwell_time_analysis dta
        JOIN txc_stops ts ON dta.naptan_id = ts.naptan_id
        {where}
        ORDER BY dta.avg_dwell_seconds DESC
        LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
    """
    
    params.extend([limit, offset])
    stops = await conn.fetch(query, *params)
    data = [RouteStopDwell(**dict(stop)) for stop in stops]
    next_url = f"/dwell-time/route/{route_name}/stops?limit={limit}&offset={offset + limit}" if offset + limit < total else None
    prev_url = f"/dwell-time/route/{route_name}/stops?limit={limit}&offset={max(0, offset - limit)}" if offset > 0 else None

    return PaginatedRouteStopDwell(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

@router.get("/stop/{naptan_id}/pattern", response_model=StopDwellPattern)
async def get_stop_dwell_pattern(
    naptan_id: str,
    route_name: str | None = Query(None),
    conn=Depends(get_db)
):
    """Get dwell time patterns for a specific stop across time"""
    
    # Get stop info
    query_stop ="""
        SELECT naptan_id, stop_name, latitude, longitude
        FROM txc_stops
        WHERE naptan_id = $1
    """
    stop_info = await conn.fetchrow(query_stop, naptan_id)
    
    if not stop_info:
        raise HTTPException(status_code=404, detail="Stop not found")
    params = [naptan_id]
    where = "WHERE naptan_id = $1"
    if route_name:
        where += " AND route_name = $2"
        params.append(route_name)
    query_route = f"""
        SELECT 
            route_name,
            direction,
            operator,
            day_of_week,
            hour_of_day,
            ROUND(avg_dwell_seconds::numeric, 1) as avg_dwell_seconds,
            ROUND(stddev_dwell_seconds::numeric, 1) as stddev_dwell_seconds,
            sample_count
        FROM dwell_time_analysis
        {where}
        ORDER BY route_name, day_of_week, hour_of_day
    """
    
    patterns = await conn.fetch(query_route, *params)
    if not patterns:        
        raise HTTPException(status_code=404, detail="No dwell time data found for this stop")
    data = [DwellPattern(**dict(p)) for p in patterns]
    
    return StopDwellPattern(
        stop=StopInfo(**dict(stop_info)),
        patterns=data,
        count=len(data)
    )

@router.get("/hotspots", response_model=Hotspots)
async def get_high_demand_stops(
    min_samples: int | None = Query(10, ge=1),
    limit: int | None = Query(20, ge=1, le=100),
    conn=Depends(get_db)
):
    """Get stops with highest average dwell times (demand proxy)"""
    
    query_hotspots ="""
        SELECT 
            dta.naptan_id,
            ts.stop_name,
            ts.latitude,
            ts.longitude,
            COUNT(DISTINCT dta.route_name) as routes_count,
            ROUND(AVG(dta.avg_dwell_seconds)::numeric, 1) as overall_avg_dwell,
            SUM(dta.sample_count) as total_samples
        FROM dwell_time_analysis dta
        JOIN txc_stops ts ON dta.naptan_id = ts.naptan_id
        GROUP BY dta.naptan_id, ts.stop_name, ts.latitude, ts.longitude
        HAVING SUM(dta.sample_count) >= $1
        ORDER BY AVG(dta.avg_dwell_seconds) DESC
        LIMIT $2
    """
    
    hotspots = await conn.fetch(query_hotspots, min_samples, limit)
    
    return Hotspots(
        hotspots=[HotspotStops(**dict(h)) for h in hotspots],
        count=len(hotspots)
    )

@router.get("/heatmap", response_model=HeatmapData)
async def get_dwell_time_heatmap(
    route_name: str,
    direction: str | None = Query(None),
    operator: str | None = Query(None),
    conn=Depends(get_db)
):
    """Get heatmap data: stops × hours with dwell times"""
    
    # Keep original for TXC query, map for dwell query
    operator_txc = operator
    operator_dwell = OPERATOR_NAME_MAP.get(operator, operator) if operator else None
    
    # Get stops on route in sequence order
    where = "WHERE rp.route_name = $1"
    params_stops = [route_name]
    
    if direction:
        where += " AND rp.direction = $" + str(len(params_stops) + 1)
        params_stops.append(direction)
    
    if operator_txc:
        where += " AND rp.operator_name = $" + str(len(params_stops) + 1)
        params_stops.append(operator_txc)
    
    query_stops = f"""
        SELECT DISTINCT
            ps.naptan_id,
            ts.stop_name,
            MIN(ps.stop_sequence) as sequence
        FROM txc_pattern_stops ps
        JOIN txc_stops ts ON ps.naptan_id = ts.naptan_id
        JOIN txc_route_patterns rp ON ps.pattern_id = rp.pattern_id
        {where}
        GROUP BY ps.naptan_id, ts.stop_name ORDER BY sequence
    """
    
    stops = await conn.fetch(query_stops, *params_stops)
    
    if not stops:
        raise HTTPException(status_code=404, detail="No stops found for this route")
    
    # Get dwell time data for heatmap
    where = "WHERE route_name = $1"
    params_heatmap = [route_name]

    if direction:
        where += " AND direction = $" + str(len(params_heatmap) + 1)
        params_heatmap.append(direction)
    
    if operator_dwell:
        where += " AND operator = $" + str(len(params_heatmap) + 1)
        params_heatmap.append(operator_dwell)
    
    query_heatmap = f"""
        SELECT 
            naptan_id,
            hour_of_day,
            ROUND(AVG(avg_dwell_seconds)::numeric, 1) as avg_dwell
        FROM dwell_time_analysis
        {where}
        GROUP BY naptan_id, hour_of_day
    """  
    
    heatmap_data = await conn.fetch(query_heatmap, *params_heatmap)
    
    # Build matrix: stops × hours
    hours = list(range(24))
    stop_ids = [s['naptan_id'] for s in stops]
    stop_names = [s['stop_name'] for s in stops]
    
    # Initialize matrix with None
    matrix = [[None for _ in hours] for _ in stop_ids]
    
    # Fill matrix with actual data
    for row in heatmap_data:
        try:
            stop_idx = stop_ids.index(row['naptan_id'])
            hour_idx = row['hour_of_day']
            matrix[stop_idx][hour_idx] = float(row['avg_dwell'])
        except (ValueError, IndexError):
            continue
    
    return HeatmapData(
        route_name=route_name,
        direction=direction,
        operator=operator,
        stops=stop_names,
        hours=hours,
        data=matrix
    )


