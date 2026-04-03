"""
Route information endpoints (TXC data only)
"""
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, literal_column, select
from sqlalchemy.orm import selectinload

from src.api.database import get_session
from src.api.models import TxcStop, TxcRoutePatterns, TxcPatternStops

class Route(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    route_name: str
    operators: str
    variants: int

class PaginatedRoutes(BaseModel):
    total: int
    limit: int
    offset: int
    next: str | None = None
    prev: str | None = None
    data: list[Route]

class RouteVariants(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    pattern_id: int
    operator_name: str
    direction: str | None = None
    origin: str | None = None
    destination: str | None = None

class RouteStops(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    naptan_id: str
    stop_name: str
    stop_sequence: int
    latitude: float
    longitude: float

class RouteDetails(BaseModel):
    route_name: str
    variant_count: int
    variants: list[RouteVariants]
    stops_in_sequence: list[RouteStops]
    
router = APIRouter(prefix="/routes", tags=["routes"])

@router.get("/", response_model = PaginatedRoutes)
async def get_all_routes(
    request: Request,
    search: str | None = Query(
        None,
        description="Search routes by name, case-insensitive"
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session=Depends(get_session)
):
    base_query = select(
        TxcRoutePatterns.route_name,
            func.string_agg(func.distinct(TxcRoutePatterns.operator_name), literal_column("', '")).label("operators"),
            func.count(TxcRoutePatterns.pattern_id.distinct()).label("variants")
        ).group_by(TxcRoutePatterns.route_name)
    
    if search:
        base_query = base_query.where(TxcRoutePatterns.route_name.ilike(f"%{search}%"))
    filtered_query = base_query.order_by(TxcRoutePatterns.route_name)
    count_query = select(func.count()).select_from(filtered_query.subquery())
    query = filtered_query.offset(offset).limit(limit)
    total = (await session.execute(count_query)).scalar()

    routes = await session.execute(query)
    rows = routes.mappings().all()
    data = [Route.model_validate(row) for row in rows]

    base = str(request.base_url).rstrip("/")
    next_url = f"{base}/routes?limit={limit}&offset={offset + limit}" if offset + limit < total else None
    prev_url = f"{base}/routes?limit={limit}&offset={max(0, offset - limit)}" if offset > 0 else None
 
    return PaginatedRoutes(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

@router.get("/{route_name}", response_model=RouteDetails)
async def get_route_details(route_name: str, session=Depends(get_session)):
    """Get route details with stops serving it"""
    query_variants = select(TxcRoutePatterns).where(TxcRoutePatterns.route_name == route_name).order_by(TxcRoutePatterns.operator_name, TxcRoutePatterns.direction)
    variants_record = await session.execute(query_variants)
    variants = variants_record.scalars().all()
    if not variants:
        raise HTTPException(status_code=404, detail="Route not found")
    
    example_variant_pattern_id = variants[0].pattern_id
    
    query_stops = await session.execute(select(TxcPatternStops).options(selectinload(TxcPatternStops.stop)).where(TxcPatternStops.pattern_id == example_variant_pattern_id).order_by(TxcPatternStops.stop_sequence))
    stop_record = query_stops.scalars().all()

    if not stop_record:
        raise HTTPException(status_code=404, detail="Route Pattern not found")

    variant_details = [RouteVariants.model_validate(v) for v in variants]
    example_stops = [RouteStops(
        naptan_id=s.naptan_id,
        stop_name=s.stop.stop_name,
        latitude=s.stop.latitude,
        longitude=s.stop.longitude,
        stop_sequence=s.stop_sequence
    ) for s in stop_record]

    return RouteDetails(
        route_name=route_name,
        variant_count=len(variant_details),
        variants=variant_details,
        stops_in_sequence=example_stops        
    )



