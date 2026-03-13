from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel
import secrets
import hashlib
from src.api.database import get_db
from src.api.auth import verify_admin

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
    request: Request,
    conn=Depends(get_db)
):
    """Get admin statistics (placeholder)"""
    total_users = await conn.fetchval("SELECT COUNT(*) FROM api_keys")
    total_active_users = await conn.fetchval("SELECT COUNT(*) FROM api_keys WHERE active = true")
    return AdminStats(total_users=total_users, total_active_users=total_active_users)
    
@router.post("/create_api_key", response_model=APIKeyResponse, include_in_schema=False, dependencies=[Depends(verify_admin)])
async def create_api_key(
    request: Request,
    api_key_data: APIKeyCreate,
    conn=Depends(get_db)
):
    """Create a new API key (placeholder)"""
    api_key = secrets.token_urlsafe(32)
    hashvalue = hashlib.sha256(api_key.encode()).hexdigest()

    await conn.execute("""
        INSERT INTO api_keys (user_name, active, hashvalue)
        VALUES ($1, $2, $3)
    """, api_key_data.user_name, True, hashvalue)

    return APIKeyResponse(user_name=api_key_data.user_name, api_key=api_key)

@router.patch("/api-keys/{user_name}/deactivate", include_in_schema=False, dependencies=[Depends(verify_admin)])
async def deactivate_api_key(
    user_name: str,
    conn=Depends(get_db)
):

    user = await conn.fetchrow("SELECT * FROM api_keys WHERE user_name = $1", user_name)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if not user['active']:
        raise HTTPException(status_code=400, detail="API key already deactivated")

    await conn.execute("UPDATE api_keys SET active = false WHERE user_name = $1", user_name)

    return {"user_name": user_name, "active": False}

@router.patch("/api-keys/{user_name}/activate", include_in_schema=False, dependencies=[Depends(verify_admin)])
async def activate_api_key(
    user_name: str,
    conn=Depends(get_db)
):

    user = await conn.fetchrow("SELECT * FROM api_keys WHERE user_name = $1", user_name)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user['active']:
        raise HTTPException(status_code=400, detail="API key already active")

    await conn.execute("UPDATE api_keys SET active = true WHERE user_name = $1", user_name)

    return {"user_name": user_name, "active": True}

@router.delete("/api-keys/{user_name}/delete", include_in_schema=False, dependencies=[Depends(verify_admin)])
async def delete_api_key(
    user_name: str,
    conn=Depends(get_db)
):

    user = await conn.fetchrow("SELECT * FROM api_keys WHERE user_name = $1", user_name)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await conn.execute("DELETE FROM api_keys WHERE user_name = $1", user_name)

    return {"detail": f"API key for user '{user_name}' deleted"}
