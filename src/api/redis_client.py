import redis.asyncio as redis
import os

client = None

async def create_client():
    global client
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = os.getenv("REDIS_PORT", "6379")
    client = redis.from_url(f"redis://{redis_host}:{redis_port}")

async def close_client():
    global client
    if client:
        await client.close()

async def get_redis():
    yield client