from collections.abc import AsyncGenerator

from redis import asyncio as aioredis
from redis.asyncio import Redis

from forge.db.config import get_settings


async def get_redis() -> AsyncGenerator[Redis, None]:
    """
    FastAPI dependency: yields a connected Redis client for the lifetime of a request.

    The client is explicitly closed after use.  Tests can override this dependency
    via ``app.dependency_overrides[get_redis]`` without touching the real Redis server.
    """
    settings = get_settings()
    client: Redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()

