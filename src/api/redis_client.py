import redis.asyncio as redis

client = None

async def create_client():
    global client
    client = redis.from_url("redis://localhost")

async def close_client():
    global client
    if client:
        await client.close()

async def get_redis():
    yield client