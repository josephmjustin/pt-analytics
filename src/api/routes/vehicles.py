from fastapi import APIRouter, Query, Depends, Request
from src.api.database import get_db
from pydantic import BaseModel
from datetime import datetime, timedelta

class VehicleDetails(BaseModel):
    vehicle_id: str
    latitude: float
    longitude: float
    bearing: float | None = None
    timestamp: datetime
    route_name: str
    direction: str | None = None
    operator: str
    origin: str | None = None
    destination: str | None = None

class PaginatedVehicles(BaseModel):
    total: int
    limit: int
    offset: int
    next: str | None = None
    prev: str | None = None
    data: list[VehicleDetails]

router = APIRouter(prefix="/vehicles", tags=["vehicles"])

@router.get("/live", response_model = PaginatedVehicles)
async def get_live_vehicles(
    request: Request,
    search: str | None = Query(
        None,
        description="Search vehicles by operator code (eg. AMSY for Arriva Merseyside, SCMY for StageCoach Merseyside,...), case-insensitive"
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_db)
):
    """Get current vehicle positions in Liverpool (last 2 minutes)"""
    cutoff_time = datetime.now() - timedelta(minutes=2)
    params = []
    where = "WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND operator IS NOT NULL"
    where += " AND timestamp >= $" + str(len(params) + 1)
    params.append(cutoff_time)

    # Bounding box
    where += " AND latitude BETWEEN $"+str(len(params)+1)+" AND $"+str(len(params)+2)
    params.extend([53.35, 53.48])

    where += " AND longitude BETWEEN $"+str(len(params)+1)+" AND $"+str(len(params)+2)
    params.extend([-3.05, -2.85])

    # Optional search filter
    if search:
        where += " AND LOWER(operator) LIKE LOWER($" + str(len(params) + 1) + ")"
        params.append(f"%{search}%")

    total = await conn.fetchval(f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (vehicle_id) vehicle_id
            FROM vehicle_positions
            {where}
            ORDER BY vehicle_id, timestamp DESC
        ) sub
    """, *params)

    query = f"""
        SELECT DISTINCT ON (vehicle_id)
            vehicle_id,
            latitude,
            longitude,
            bearing,
            timestamp,
            route_name,
            direction,
            operator,
            origin,
            destination
        FROM vehicle_positions
        {where}
        ORDER BY vehicle_id, timestamp DESC
        LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
    """
    params.extend([limit, offset])

    vehicles = await conn.fetch(query, *params)
    data = [VehicleDetails(**dict(v)) for v in vehicles]

    base = str(request.base_url).rstrip("/")
    next_url = f"{base}/vehicles/live?limit={limit}&offset={offset + limit}" if offset + limit < total else None
    prev_url = f"{base}/vehicles/live?limit={limit}&offset={max(0, offset - limit)}" if offset > 0 else None

    return PaginatedVehicles(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

