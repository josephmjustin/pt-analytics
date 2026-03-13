from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from .database import get_db
import os
import dotenv
import hashlib

dotenv.load_dotenv()

api_key_header = APIKeyHeader(name="PTAnalytics-API-Key")
admin_password_header = APIKeyHeader(name="PTAnalytics-Admin-Password")


async def verify_api_key(
    api_key: str = Security(api_key_header),
    conn=Depends(get_db)
):
    hashvalue = hashlib.sha256(api_key.encode()).hexdigest()
    result = await conn.fetchval(
        "SELECT 1 FROM api_keys WHERE hashvalue = $1 AND active = true", hashvalue
    )
    if not result:
        raise HTTPException(status_code=401, detail="Invalid API key")

async def verify_admin(admin_password: str = Security(admin_password_header)):
    if admin_password != os.getenv("ADMIN_PASSWORD"):
        raise HTTPException(status_code=401, detail="Invalid admin password")
    return admin_password