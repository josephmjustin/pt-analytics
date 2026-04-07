from fastapi import APIRouter, Query, Depends, Request
from pydantic import BaseModel, ConfigDict
from datetime import datetime, timedelta, timezone
from sqlalchemy import func, select

from src.api.auth import verify_api_key
from src.api.database import get_session
from src.api.models import VehiclePositions

class VehicleDetails(BaseModel):
    model_config = ConfigDict(from_attributes=True)
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

@router.get("/live", response_model = PaginatedVehicles, dependencies=[Depends(verify_api_key)])
async def get_live_vehicles(
    request: Request,
    search: str | None = Query(
        None,
        description="Search vehicles by operator code (eg. AMSY for Arriva Merseyside, SCMY for StageCoach Merseyside,...), case-insensitive"
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session=Depends(get_session)
):
    """Get current vehicle positions in Liverpool (last 2 minutes)"""
    cutoff_time = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=2)
    
    base_query = select(VehiclePositions)
    if search:
        base_query = base_query.where(VehiclePositions.operator.ilike(f"%{search}%"))
    filtered_query = base_query.where(
        VehiclePositions.latitude.is_not(None),
        VehiclePositions.longitude.is_not(None),
        VehiclePositions.operator.is_not(None),
        VehiclePositions.timestamp >= cutoff_time,
        VehiclePositions.latitude.between(53.35, 53.48),
        VehiclePositions.longitude.between(-3.05, -2.85),
    ).distinct(VehiclePositions.vehicle_id).order_by(VehiclePositions.vehicle_id, VehiclePositions.timestamp.desc())
    count_query = select(func.count()).select_from(filtered_query.subquery())
    query = filtered_query.offset(offset).limit(limit)
    total = (await session.execute(count_query)).scalar()

    vehicles = await session.execute(query)
    rows = vehicles.scalars().all()
    data = [VehicleDetails.model_validate(v) for v in rows]

    base = str(request.base_url).rstrip("/")
    next_url = f"{base}/vehicles/live?limit={limit}&offset={offset + limit}" if offset + limit < total else None
    prev_url = f"{base}/vehicles/live?limit={limit}&offset={max(0, offset - limit)}" if offset > 0 else None

    return PaginatedVehicles(total=total, limit=limit, offset=offset, next=next_url, prev=prev_url, data=data)

