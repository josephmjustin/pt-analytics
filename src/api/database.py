import asyncpg
import os
import ssl as ssl_module

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from dotenv import load_dotenv

load_dotenv()

DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
DB_URL_ALCHEMY = f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

pool: asyncpg.Pool | None = None

async def create_pool():
    global pool
    ssl_context = None
    if os.getenv("DB_SSL", "false") == "true":
        ssl_context = ssl_module.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl_module.CERT_NONE
    pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20, ssl=ssl_context)

async def close_pool():
    global pool
    if pool:
        await pool.close()

async def get_db():
    async with pool.acquire() as conn:
        yield conn

# Add connection for sqlalchemy
engine = create_async_engine(DB_URL_ALCHEMY, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)
async def get_session():
    async with async_session() as session:
        yield session