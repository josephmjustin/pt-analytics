import secrets
import hashlib
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import select, func
from src.api.database import get_session
from src.api.auth import verify_admin
from src.api.models import ApiKeys

class AdminStats(BaseModel):
    total_users: int
    total_active_users: int

class APIKeyCreate(BaseModel):
    user_name: str

class APIKeyResponse(BaseModel):
    user_name: str
    api_key: str

router = APIRouter(prefix="/admin", tags=["admin"])

@router.get("/stats", response_model=AdminStats, include_in_schema=False, dependencies=[Depends(verify_admin)])
async def get_admin_stats(
    session=Depends(get_session)
):
    """Get admin statistics (placeholder)"""
    total_users_query = select(func.count()).select_from(ApiKeys)
    total_active_users_query = select(func.count()).select_from(ApiKeys).where(ApiKeys.active)
    total_users = (await session.execute(total_users_query)).scalar()
    total_active_users = (await session.execute(total_active_users_query)).scalar()
    
    return AdminStats(total_users=total_users, total_active_users=total_active_users)
    
@router.post("/create_api_key", response_model=APIKeyResponse, include_in_schema=False, dependencies=[Depends(verify_admin)])
async def create_api_key(
    api_key_data: APIKeyCreate,
    session=Depends(get_session)
):
    """Create a new API key (placeholder)"""
    api_key = secrets.token_urlsafe(32)
    hashvalue = hashlib.sha256(api_key.encode()).hexdigest()

    new_key = ApiKeys(user_name=api_key_data.user_name, hashvalue=hashvalue)
    session.add(new_key)
    await session.commit()

    return APIKeyResponse(user_name=api_key_data.user_name, api_key=api_key)

@router.patch("/api-keys/{user_name}/deactivate", include_in_schema=False, dependencies=[Depends(verify_admin)])
async def deactivate_api_key(
    user_name: str,
    session=Depends(get_session)
):
    query_user = select(ApiKeys).where(ApiKeys.user_name == user_name)
    user = (await session.execute(query_user)).scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if not user.active:
        raise HTTPException(status_code=400, detail="API key already deactivated")

    user.active = False
    await session.commit()

    return {"user_name": user.user_name, "active": user.active}

@router.patch("/api-keys/{user_name}/activate", include_in_schema=False, dependencies=[Depends(verify_admin)])
async def activate_api_key(
    user_name: str,
    session=Depends(get_session)
):
    query_user = select(ApiKeys).where(ApiKeys.user_name == user_name)
    user = (await session.execute(query_user)).scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user.active:
        raise HTTPException(status_code=400, detail="API key already activated")

    user.active = True
    await session.commit()

    return {"user_name": user.user_name, "active": user.active}

@router.delete("/api-keys/{user_name}/delete", include_in_schema=False, dependencies=[Depends(verify_admin)])
async def delete_api_key(
    user_name: str,
    session=Depends(get_session)
):
    query_user = select(ApiKeys).where(ApiKeys.user_name == user_name)
    user = (await session.execute(query_user)).scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await session.delete(user)
    await session.commit()
    return {"detail": f"API key for user '{user_name}' deleted"}