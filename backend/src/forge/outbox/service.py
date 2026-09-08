from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from forge.db.models import OutboxEvent
from forge.messaging.redis_streams import SchedulingEvent


def scheduling_event_payload(event: SchedulingEvent) -> dict[str, str]:
    """Build the Redis-compatible payload stored in an outbox event."""

    return {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "job_id": str(event.job_id),
        "created_at": event.created_at.isoformat(),
    }


def add_outbox_event(
    session: AsyncSession,
    *,
    event_type: str,
    payload: dict[str, Any],
) -> OutboxEvent:
    """Add an outbox event to the caller's transaction without committing it."""

    event = OutboxEvent(event_type=event_type, payload=payload)
    session.add(event)
    return event
