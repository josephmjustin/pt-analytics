import os
import dotenv
import hashlib
from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from .database import get_session
from src.api.models import ApiKeys

dotenv.load_dotenv()

api_key_header = APIKeyHeader(name="PTAnalytics-API-Key")
admin_password_header = APIKeyHeader(name="PTAnalytics-Admin-Password")


async def verify_api_key(
    api_key: str = Security(api_key_header),
    session=Depends(get_session)
):
    hashvalue = hashlib.sha256(api_key.encode()).hexdigest()
    query = select(ApiKeys.id).where(ApiKeys.hashvalue == hashvalue, ApiKeys.active)
    result = (await session.execute(query)).scalar_one_or_none()

    if not result:
        raise HTTPException(status_code=401, detail="Invalid API key")

async def verify_admin(admin_password: str = Security(admin_password_header)):
    if admin_password != os.getenv("ADMIN_PASSWORD"):
        raise HTTPException(status_code=401, detail="Invalid admin password")
    return admin_password