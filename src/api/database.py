import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

pool: asyncpg.Pool | None = None

async def create_pool():
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)

async def close_pool():
    global pool
    if pool:
        await pool.close()

async def get_db():
    async with pool.acquire() as conn:
        yield conn