from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from forge.db.models import OutboxEvent
from forge.messaging.redis_streams import RedisStreams, SchedulingEvent


class OutboxPublisher:
    def __init__(
        self,
        session: AsyncSession,
        redis_streams: RedisStreams,
    ) -> None:
        self.session = session
        self.redis_streams = redis_streams

    async def get_pending_events(self) -> list[OutboxEvent]:
        result = await self.session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at.asc())
        )
        return list(result.scalars().all())

    def _to_scheduling_event(self, outbox_event: OutboxEvent) -> SchedulingEvent:
        payload = outbox_event.payload

        return SchedulingEvent(
            event_id=UUID(payload["event_id"]),
            event_type=payload["event_type"],
            job_id=UUID(payload["job_id"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
        )

    def _record_failure(self, outbox_event: OutboxEvent, exc: Exception) -> None:
        outbox_event.retry_count += 1
        outbox_event.last_error = str(exc)

    async def publish_pending(self) -> int:
        events = await self.get_pending_events()

        published_count = 0

        for outbox_event in events:
            try:
                event = self._to_scheduling_event(outbox_event)

                await self.redis_streams.publish(event)

                outbox_event.published_at = datetime.now(UTC)
                published_count += 1

            except Exception as exc:
                self._record_failure(outbox_event, exc)

        await self.session.commit()

        return published_count
