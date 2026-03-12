"""
Route information endpoints (TXC data only)
"""
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from src.api.database import get_db
from pydantic import BaseModel

class Route(BaseModel):
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
    pattern_id: int
    operator_name: str
    direction: str | None = None
    origin: str | None = None
    destination: str | None = None

class RouteStops(BaseModel):
    naptan_id: str
    stop_name: str
    stop_sequence: int
    latitude: float
    longitude: float

class RouteDetails(BaseModel):
    route_name: str
    variants: list[RouteVariants]
    stops_in_sequence: list[RouteStops]
    variant_count: int

router = APIRouter(prefix="/routes", tags=["routes"])

@router.get("/", response_model = PaginatedRoutes)
async def get_all_routes(
    request: Request,
    search: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_db)
):
    params = []
    where = "WHERE route_name IS NOT NULL"

    if search:
        where += " AND LOWER(route_name) LIKE LOWER($" + str(len(params) + 1) + ")"
        params.append(f"%{search}%")

    total = await conn.fetchval(f"SELECT COUNT(DISTINCT route_name) FROM txc_route_patterns {where}", *params)

    query = f"""
        SELECT DISTINCT
            route_name,
            STRING_AGG(DISTINCT operator_name, ', ') as operators,
            COUNT(DISTINCT pattern_id) as variants
        FROM txc_route_patterns {where}
        GROUP BY route_name
        ORDER BY route_name
        LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
    """
    params.extend([limit, offset])

    routes = await conn.fetch(query, *params)
    data = [Route(**dict(r)) for r in routes]

    base = str(request.base_url).rstrip("/")
    next_url = f"{base}/routes?limit={limit}&offset={offset + limit}" if offset + limit < total else None
    prev_url = f"{base}/routes?limit={limit}&offset={max(0, offset - limit)}" if offset > 0 else None

    return PaginatedRoutes(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

@router.get("/{route_name}", response_model=RouteDetails)
async def get_route_details(route_name: str, conn=Depends(get_db)):
    """Get route details with stops serving it"""
    query_variants = """
        SELECT DISTINCT
            pattern_id,
            operator_name,
            direction,
            origin,
            destination
        FROM txc_route_patterns
        WHERE route_name = $1
        ORDER BY operator_name, direction
    """
    query_stops = """
         SELECT 
            ps.naptan_id,
            ts.stop_name,
            ps.stop_sequence,
            ts.latitude,
            ts.longitude
        FROM txc_pattern_stops ps
        JOIN txc_stops ts ON ps.naptan_id = ts.naptan_id
        WHERE ps.pattern_id = $1
        ORDER BY ps.stop_sequence
    """
    variants_record = await conn.fetch(query_variants, route_name)
    if not variants_record:
        raise HTTPException(status_code=404, detail="Route not found")
    
    example_variant_pattern_id = variants_record[0]["pattern_id"]
    stop_record = await conn.fetch(query_stops, example_variant_pattern_id)
    if not stop_record:
        raise HTTPException(status_code=404, detail="Route Pattern not found")

    variants = [RouteVariants(**dict(v)) for v in variants_record]
    example_stops = [RouteStops(**dict(s)) for s in stop_record]

    return RouteDetails(
        route_name=route_name,
        variants=variants,
        stops_in_sequence=example_stops,
        variant_count=len(variants)
    )


