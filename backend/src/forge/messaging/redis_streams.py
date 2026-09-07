from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from redis import asyncio as redis

from forge.db.config import get_settings


@dataclass(frozen=True)
class SchedulingEvent:
    event_id: UUID
    event_type: str
    job_id: UUID
    created_at: datetime


class RedisStreams:
    def __init__(self) -> None:
        self.client: redis.Redis | None = None

    async def connect(self) -> None:
        settings = get_settings()
        self.client = redis.from_url(
            settings.redis_url,
            decode_responses=True,
        )

    async def health_check(self) -> bool:
        if self.client is None:
            raise RuntimeError("Redis client is not connected.")
        try:
            pong = await self.client.ping()
            return pong
        except redis.ConnectionError:
            return False

    async def publish(self, event: SchedulingEvent) -> str:
        if self.client is None:
            raise RuntimeError("Redis client is not connected.")

        settings = get_settings()

        message_id = await self.client.xadd(
            settings.redis_stream_name,
            {
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "job_id": str(event.job_id),
                "created_at": event.created_at.isoformat(),
            },
        )

        return message_id

    async def create_consumer_group(self) -> None:
        if self.client is None:
            raise RuntimeError("Redis client is not connected.")

        settings = get_settings()

        try:
            await self.client.xgroup_create(
                name=settings.redis_stream_name,
                groupname=settings.redis_consumer_group,
                id="0",
                mkstream=True,
            )
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read_group(
        self,
        consumer_name: str,
        count: int = 10,
        block_ms: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        if self.client is None:
            raise RuntimeError("Redis client is not connected.")

        settings = get_settings()

        messages = await self.client.xreadgroup(
            groupname=settings.redis_consumer_group,
            consumername=consumer_name,
            streams={settings.redis_stream_name: ">"},
            count=count,
            block=block_ms,
        )

        return [
            (message_id, fields)
            for _, stream_messages in messages
            for message_id, fields in stream_messages
        ]

    async def acknowledge(self, message_id: str) -> int:
        if self.client is None:
            raise RuntimeError("Redis client is not connected.")

        settings = get_settings()

        return await self.client.xack(
            settings.redis_stream_name,
            settings.redis_consumer_group,
            message_id,
        )

    async def pending(self) -> list[dict[str, str | int]]:
        if self.client is None:
            raise RuntimeError("Redis client is not connected.")

        settings = get_settings()

        return await self.client.xpending_range(
            settings.redis_stream_name,
            settings.redis_consumer_group,
            min="-",
            max="+",
            count=10,
        )

    async def close(self) -> None:
        if self.client is None:
            return

        client = self.client
        self.client = None
        await client.aclose()

    @staticmethod
    def deserialize_event(fields: dict[str, str]) -> SchedulingEvent:
        return SchedulingEvent(
            event_id=UUID(fields["event_id"]),
            event_type=fields["event_type"],
            job_id=UUID(fields["job_id"]),
            created_at=datetime.fromisoformat(fields["created_at"]),
        )
