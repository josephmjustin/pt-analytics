"""
Dwell Time Analysis API Endpoints
Provides demand proxy insights based on dwell time patterns
"""
import json
from fastapi import APIRouter, HTTPException, Request, Response, Query, Depends
from pydantic import BaseModel, ConfigDict
from typing import Generic, TypeVar, Optional, List
from sqlalchemy import func, select, cast, Numeric
from src.api.database import get_session
from src.api.auth import verify_api_key
from src.api.rate_limiter import limiter
from src.api.redis_client import get_redis
from src.api.models import DwellTimeAnalysis, TxcRoutePatterns, TxcStop, TxcPatternStops
from src.api.pagination import build_pagination_links


T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    limit: int
    offset: int
    next: Optional[str] = None
    prev: Optional[str] = None
    data: List[T]

class DwellTimeStats(BaseModel):
    model_config = ConfigDict(from_attributes=True)
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
    model_config = ConfigDict(from_attributes=True)
    route_name: str
    stops_with_data: int
    operators: int
    total_samples: int
    avg_dwell: float

class RouteStopDwell(BaseModel):
    model_config = ConfigDict(from_attributes=True)
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
    model_config = ConfigDict(from_attributes=True)
    route_name: str
    direction: str | None = None
    operator: str
    day_of_week: int
    hour_of_day: int
    avg_dwell_seconds: float
    stddev_dwell_seconds: float | None = None
    sample_count: int

class StopInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    naptan_id: str
    stop_name: str
    latitude: float
    longitude: float

class StopDwellPattern(BaseModel):
    stop: StopInfo
    patterns: list[DwellPattern]
    count: int

class HotspotStops(BaseModel):
    model_config = ConfigDict(from_attributes=True)
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
    model_config = ConfigDict(from_attributes=True)
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

def build_cache_key(endpoint, **params):
    key = f"{endpoint}:" + ":".join(f"{k}={v}" for k, v in sorted(params.items()) if v is not None)
    return key

@router.get("/stats", response_model=DwellTimeStats)
async def get_dwell_time_stats(session=Depends(get_session)):
    """Get overall dwell time statistics"""
    query = select(
        func.count(func.distinct(DwellTimeAnalysis.naptan_id)).label("unique_stops"),
        func.count(func.distinct(DwellTimeAnalysis.route_name)).label("unique_routes"),
        func.count(func.distinct(DwellTimeAnalysis.operator)).label("unique_operators"),
        func.sum(DwellTimeAnalysis.sample_count).label("total_samples"),
        func.round(cast(func.avg(DwellTimeAnalysis.avg_dwell_seconds), Numeric), 1).label("overall_avg_dwell"),
        func.round(cast(func.min(DwellTimeAnalysis.avg_dwell_seconds), Numeric), 1).label("min_avg_dwell"),
        func.round(cast(func.max(DwellTimeAnalysis.avg_dwell_seconds), Numeric), 1).label("max_avg_dwell"),
    )

    result = await session.execute(query)
    row = result.mappings().one()

    return DwellTimeStats.model_validate(row)

@router.get("/filters", response_model=FilterOptions)
async def get_filter_options(session=Depends(get_session)):
    """Get available filter options for dropdowns"""
    # Get unique operators
    query_operators = select(TxcRoutePatterns.operator_name).distinct().order_by(TxcRoutePatterns.operator_name)
    result = await session.execute(query_operators)
    operators = result.scalars().all()

    # Get unique directions
    query_directions = select(TxcRoutePatterns.direction).where(TxcRoutePatterns.direction.is_not(None)).distinct().order_by(TxcRoutePatterns.direction)
    result = await session.execute(query_directions)
    directions = result.scalars().all()

    return FilterOptions(operators=operators, directions= directions)

@router.get("/routes", response_model=PaginatedDwellRoutes, dependencies=[Depends(verify_api_key)])
async def get_routes_with_dwell_data(
    request: Request,
    search: str | None = Query(
        None,
        description="Search routes by name, case-insensitive"
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session=Depends(get_session)
):
    """Get all routes with dwell time data available"""
    # Base aggregated query
    base_query = (
        select(
            DwellTimeAnalysis.route_name,
            func.count(func.distinct(DwellTimeAnalysis.naptan_id)).label("stops_with_data"),
            func.count(func.distinct(DwellTimeAnalysis.operator)).label("operators"),
            func.sum(DwellTimeAnalysis.sample_count).label("total_samples"),
            func.round(cast(func.avg(DwellTimeAnalysis.avg_dwell_seconds), Numeric), 1).label("avg_dwell"),
        )
    )

    # Apply filter
    if search:
        base_query = base_query.where(
            DwellTimeAnalysis.route_name.ilike(f"%{search}%")
        )

    # Group + order
    filtered_query = base_query.group_by(DwellTimeAnalysis.route_name).order_by(
        DwellTimeAnalysis.route_name
    )

    # Pagination
    query = filtered_query.offset(offset).limit(limit)

    # Count total groups (routes)
    count_query = select(func.count()).select_from(filtered_query.subquery())
    total = (await session.execute(count_query)).scalar()

    # Execute main query
    result = await session.execute(query)
    rows = result.mappings().all()

    # Unpack to data
    data = [RouteDwellSummary.model_validate(row) for row in rows]

    next_url, prev_url = build_pagination_links(request, offset, limit, total)

    return PaginatedDwellRoutes(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

@router.get("/route/{route_name}/stops", response_model=PaginatedRouteStopDwell, dependencies=[Depends(verify_api_key)])
async def get_route_stops_dwell(
    request: Request,
    route_name: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    direction: str | None = Query(None, description="Filter by direction, lowercase (outbound/ inbound)"),
    operator: str | None = Query(None, description="Filter by operator, case sensitive, should match exactly (eg. Arriva, Stagecoach, ...)"),
    day_of_week: int | None = Query(None, ge=0, le=6, description="Filter by day of week (0=Monday, 6=Sunday)"),
    hour_of_day: int | None = Query(None, ge=0, le=23, description="Filter by hour of day (0-23)"),
    session=Depends(get_session)
):
    """Get dwell time analysis for stops on a route"""
    base_query = (
        select(
            DwellTimeAnalysis.naptan_id,
            DwellTimeAnalysis.direction,
            DwellTimeAnalysis.operator,
            DwellTimeAnalysis.day_of_week,
            DwellTimeAnalysis.hour_of_day,
            DwellTimeAnalysis.sample_count,
            func.round(cast(DwellTimeAnalysis.avg_dwell_seconds, Numeric), 1).label("avg_dwell_seconds"),
            func.round(cast(DwellTimeAnalysis.stddev_dwell_seconds, Numeric), 1).label("stddev_dwell_seconds"),
            TxcStop.stop_name,
            TxcStop.latitude,
            TxcStop.longitude
        ).join(
            TxcStop, DwellTimeAnalysis.naptan_id == TxcStop.naptan_id
            ).where(
                DwellTimeAnalysis.route_name ==route_name
                )
    )

    # Apply filters
    if direction:
        base_query = base_query.where(
            DwellTimeAnalysis.direction == direction
        )

    if operator:
        base_query = base_query.where(
            DwellTimeAnalysis.operator == operator
        )

    if day_of_week is not None:
        base_query = base_query.where(
            DwellTimeAnalysis.day_of_week == day_of_week
        )

    if hour_of_day is not None:
        base_query = base_query.where(
            DwellTimeAnalysis.hour_of_day == hour_of_day
        )
    
    # Order
    filtered_query = base_query.order_by(
        DwellTimeAnalysis.avg_dwell_seconds.desc()
    )

    # Pagination
    query = filtered_query.offset(offset).limit(limit)

    # Count total groups (routes)
    count_query = select(func.count()).select_from(filtered_query.subquery())
    total = (await session.execute(count_query)).scalar()

    # Execute main query
    result = await session.execute(query)
    rows = result.mappings().all()

    # Unpack to data
    data = [RouteStopDwell.model_validate(row) for row in rows]

    next_url, prev_url = build_pagination_links(request, offset, limit, total)

    return PaginatedRouteStopDwell(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

@router.get("/stop/{naptan_id}/pattern", response_model=StopDwellPattern, dependencies=[Depends(verify_api_key)])
async def get_stop_dwell_pattern(
    naptan_id: str,
    route_name: str | None = Query(None),
    session=Depends(get_session)
):
    """Get dwell time patterns for a specific stop across time"""
    
    # Get stop info
    query_stop =select(
        TxcStop
    ).where(TxcStop.naptan_id == naptan_id)
    
    result = await session.execute(query_stop)
    stop_info = result.scalars().one_or_none()
    
    if not stop_info:
        raise HTTPException(status_code=404, detail="Stop not found")
    
    base_query = select(
        DwellTimeAnalysis.route_name,
        DwellTimeAnalysis.direction,
        DwellTimeAnalysis.operator,
        DwellTimeAnalysis.day_of_week,
        DwellTimeAnalysis.hour_of_day,
        DwellTimeAnalysis.sample_count,
        func.round(cast(DwellTimeAnalysis.avg_dwell_seconds, Numeric), 1).label("avg_dwell_seconds"),
        func.round(cast(DwellTimeAnalysis.stddev_dwell_seconds, Numeric), 1).label("stddev_dwell_seconds")
    ).where(DwellTimeAnalysis.naptan_id == naptan_id)
    
    if route_name:
        base_query = base_query.where(DwellTimeAnalysis.route_name == route_name)
    
    filtered_query = base_query.order_by(DwellTimeAnalysis.route_name, DwellTimeAnalysis.day_of_week, DwellTimeAnalysis.hour_of_day)

    result = await session.execute(filtered_query)
    data = result.mappings().all()

    if not data:
        if route_name:
            raise HTTPException(status_code=404, detail="Route not found for this stop")
        else:
            raise HTTPException(status_code=404, detail="No dwell time data found for this stop")
    
    return StopDwellPattern(
        stop=StopInfo.model_validate(stop_info),
        patterns=data,
        count=len(data)
    )

@router.get("/hotspots", response_model=Hotspots)
@limiter.limit("5/minute")
async def get_high_demand_stops(
    request: Request,
    response: Response,
    min_samples: int | None = Query(10, ge=1),
    limit: int | None = Query(20, ge=1, le=100),
    session=Depends(get_session),
    redis = Depends(get_redis)
):
    key = build_cache_key("hotspots", min_samples=min_samples, limit=limit)
    cached = await redis.get(key)
    if cached:
        response.headers["X-Cache"] = "HIT"
        return Hotspots(**json.loads(cached))
    else:
        """Get stops with highest average dwell times (demand proxy)"""
        query_hotspots = select(
            DwellTimeAnalysis.naptan_id,
            TxcStop.stop_name,
            TxcStop.latitude,
            TxcStop.longitude,
            func.count(func.distinct(DwellTimeAnalysis.route_name)).label("routes_count"),
            func.round(cast(func.avg(DwellTimeAnalysis.avg_dwell_seconds), Numeric), 1).label("overall_avg_dwell"),
            func.sum(DwellTimeAnalysis.sample_count).label("total_samples")
        ).join(
            TxcStop, DwellTimeAnalysis.naptan_id == TxcStop.naptan_id
        ).group_by(
            DwellTimeAnalysis.naptan_id, TxcStop.stop_name, TxcStop.latitude, TxcStop.longitude
        ).having(
            func.sum(DwellTimeAnalysis.sample_count) >= min_samples
        ).order_by(func.avg(DwellTimeAnalysis.avg_dwell_seconds).desc()).limit(limit)

        # Execute main query
        hotspots = await session.execute(query_hotspots)
        rows = hotspots.mappings().all()
        result = Hotspots(
            hotspots=rows,
            count=len(rows)
        )
        await redis.set(key, json.dumps(result.model_dump()), ex=3600)  # Cache for 1 hour
        response.headers["X-Cache"] = "MISS"
        return result

@router.get("/heatmap", response_model=HeatmapData, dependencies=[Depends(verify_api_key)])
async def get_dwell_time_heatmap(
    route_name: str,
    response: Response,
    direction: str | None = Query(None, description="Filter by direction, lowercase (outbound/ inbound)"),
    operator: str | None = Query(None, description="Full operator name as returned by /dwell-time/filters endpoint (e.g., 'Arriva Merseyside', not 'Arriva')"),
    session=Depends(get_session),
    redis = Depends(get_redis)
):
    """Get heatmap data: stops × hours with dwell times"""
    
    # Keep original for TXC query, map for dwell query
    operator_txc = operator
    operator_dwell = OPERATOR_NAME_MAP.get(operator, operator) if operator else None
    
    key_heatmap = build_cache_key("heatmap", route_name=route_name, direction=direction, operator=operator_dwell)
    cached_heatmap = await redis.get(key_heatmap)
    if cached_heatmap:
        response.headers["X-Cache"] = "HIT"
        return HeatmapData(**json.loads(cached_heatmap))
    else:
        base_query_stops = (
            select(
                TxcPatternStops.naptan_id,
                TxcStop.stop_name,
                func.min(TxcPatternStops.stop_sequence).label("sequence"),
            )
            .join(TxcStop, TxcPatternStops.naptan_id == TxcStop.naptan_id)
            .join(TxcRoutePatterns, TxcPatternStops.pattern_id == TxcRoutePatterns.pattern_id)
            .where(TxcRoutePatterns.route_name == route_name)
        )
        
        base_query_heatmap = select(
            DwellTimeAnalysis.naptan_id,
            DwellTimeAnalysis.hour_of_day,
            func.round(cast(func.avg(DwellTimeAnalysis.avg_dwell_seconds), Numeric), 1).label("avg_dwell")
        ).where(
            DwellTimeAnalysis.route_name == route_name
        )
        
        if direction:
            base_query_stops = base_query_stops.where(TxcRoutePatterns.direction == direction)
            base_query_heatmap = base_query_heatmap.where(DwellTimeAnalysis.direction == direction)
        
        if operator:
            base_query_stops = base_query_stops.where(TxcRoutePatterns.operator_name == operator_txc)
            base_query_heatmap = base_query_heatmap.where(DwellTimeAnalysis.operator == operator_dwell)
        
        query_stops = base_query_stops.group_by(
            TxcPatternStops.naptan_id, TxcStop.stop_name
            ).order_by(
                "sequence"
            )

        stops = await session.execute(query_stops)
        rows_stops = stops.mappings().all()

        if not rows_stops:
            raise HTTPException(status_code=404, detail="No stops found for this route")
        
        # Get dwell time data for heatmap
        query_heatmap = base_query_heatmap.group_by(
            DwellTimeAnalysis.naptan_id,
            DwellTimeAnalysis.hour_of_day
        )
        
        heatmap_data = await session.execute(query_heatmap)
        rows_heatmap = heatmap_data.mappings().all()

        # Build matrix: stops × hours
        hours = list(range(24))
        stop_ids = [s['naptan_id'] for s in rows_stops]
        stop_names = [s['stop_name'] for s in rows_stops]
        
        # Initialize matrix with None
        matrix = [[None for _ in hours] for _ in stop_ids]
        
        # Fill matrix with actual data
        for row in rows_heatmap:
            try:
                stop_idx = stop_ids.index(row['naptan_id'])
                hour_idx = row['hour_of_day']
                matrix[stop_idx][hour_idx] = float(row['avg_dwell'])
            except (ValueError, IndexError):
                continue
        
        result = HeatmapData(
            route_name=route_name,
            direction=direction,
            operator=operator,
            stops=stop_names,
            hours=hours,
            data=matrix
        )        

        await redis.set(key_heatmap, json.dumps(result.model_dump()), ex=3600)  # Cache heatmap data for 1 hour
        response.headers["X-Cache"] = "MISS"
        return result
