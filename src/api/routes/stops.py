"""
Stop information endpoints (TXC data only)
"""
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select

from src.api.database import get_session
from src.api.models import TxcStop, TxcRoutePatterns, TxcPatternStops

class Stop(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    naptan_id: str
    stop_name: str
    latitude: float
    longitude: float

class PaginatedStops(BaseModel):
    total: int
    limit: int
    offset: int
    next: str | None = None
    prev: str | None = None
    data: list[Stop]

class Route(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    route_name: str
    operator_name: str
    direction: str | None = None

class StopDetail(BaseModel):
    stop: Stop
    route: list[Route]
    route_count: int

router = APIRouter(prefix="/stops", tags=["stops"])

@router.get("/", response_model=PaginatedStops)
async def get_all_stops(
    request: Request,
    search: str | None = Query(
        None,
        description="Search stop by name, case-insensitive",
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session=Depends(get_session)
):
    base_query = select(TxcStop)
    if search:
        base_query = base_query.where(TxcStop.stop_name.ilike(f"%{search}%"))
    filtered_query = base_query.order_by(TxcStop.stop_name).where(TxcStop.latitude.is_not(None), TxcStop.longitude.is_not(None))
    count_query = select(func.count()).select_from(filtered_query.subquery())
    query = filtered_query.offset(offset).limit(limit)
    total = (await session.execute(count_query)).scalar()

    stops = await session.execute(query)
    rows = stops.scalars().all()
    data = [Stop.model_validate(row) for row in rows]

    base = str(request.base_url).rstrip("/")
    next_url = f"{base}/stops?limit={limit}&offset={offset + limit}" if offset + limit < total else None
    prev_url = f"{base}/stops?limit={limit}&offset={max(0, offset - limit)}" if offset > 0 else None

    return PaginatedStops(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

@router.get("/{stop_id}", response_model=StopDetail)
async def get_stop_details(stop_id: str, session=Depends(get_session)):
    """Get stop details with routes serving it"""
    query_stop = await session.execute(select(TxcStop).where(TxcStop.naptan_id == stop_id))
    stop_record = query_stop.scalar_one_or_none()
    if not stop_record:
        raise HTTPException(status_code=404, detail="Stop not found")
    query_route = await session.execute((select(TxcRoutePatterns).where(TxcRoutePatterns.pattern_stops.any(TxcPatternStops.naptan_id == stop_id)).distinct()).order_by(TxcRoutePatterns.route_name, TxcRoutePatterns.direction))
    route_records = query_route.scalars().all()
    stop = Stop.model_validate(stop_record)
    routes = [Route.model_validate(r) for r in route_records]

    return StopDetail(
        stop=stop,
        route=routes,
        route_count=len(routes)
    )
