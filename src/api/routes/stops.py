"""
Stop information endpoints (TXC data only)
"""
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from src.api.database import get_db
from pydantic import BaseModel

class Stop(BaseModel):
    stop_id: str
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
    conn=Depends(get_db)
):
    params = []
    where = "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"

    if search:
        where += " AND LOWER(stop_name) LIKE LOWER($" + str(len(params) + 1) + ")"
        params.append(f"%{search}%")

    total = await conn.fetchval(f"SELECT COUNT(*) FROM txc_stops {where}", *params)

    query = f"""
        SELECT naptan_id as stop_id, stop_name, latitude, longitude
        FROM txc_stops {where}
        ORDER BY stop_name
        LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
    """
    params.extend([limit, offset])

    stops = await conn.fetch(query, *params)
    data = [Stop(**dict(s)) for s in stops]

    base = str(request.base_url).rstrip("/")
    next_url = f"{base}/stops?limit={limit}&offset={offset + limit}" if offset + limit < total else None
    prev_url = f"{base}/stops?limit={limit}&offset={max(0, offset - limit)}" if offset > 0 else None

    return PaginatedStops(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

@router.get("/{stop_id}", response_model=StopDetail)
async def get_stop_details(stop_id: str, conn=Depends(get_db)):
    """Get stop details with routes serving it"""
    query_stop = """
        SELECT naptan_id as stop_id, stop_name, latitude, longitude
        FROM txc_stops
        WHERE naptan_id = $1
    """
    query_route = """
        SELECT DISTINCT
            rp.route_name,
            rp.operator_name,
            rp.direction
        FROM txc_pattern_stops ps
        JOIN txc_route_patterns rp ON ps.pattern_id = rp.pattern_id
        WHERE ps.naptan_id = $1
        ORDER BY rp.route_name, rp.direction
    """
    stop_record = await conn.fetchrow(query_stop, stop_id)
    if not stop_record:
        raise HTTPException(status_code=404, detail="Stop not found")
    route_records = await conn.fetch(query_route, stop_id)
    stop = Stop(**dict(stop_record))
    routes = [Route(**dict(r)) for r in route_records]

    return StopDetail(
        stop=stop,
        route=routes,
        route_count=len(routes)
    )