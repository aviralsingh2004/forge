from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio

from forge.db.config import get_settings
from forge.messaging.redis_streams import RedisStreams, SchedulingEvent


@pytest_asyncio.fixture
async def redis_streams(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[RedisStreams, None]:
    suffix = uuid4().hex
    monkeypatch.setenv("REDIS_STREAM_NAME", f"forge:scheduling:test:{suffix}")
    monkeypatch.setenv("REDIS_CONSUMER_GROUP", f"scheduler-group:test:{suffix}")
    get_settings.cache_clear()

    streams = RedisStreams()
    await streams.connect()
    if not await streams.health_check():
        await streams.close()
        get_settings.cache_clear()
        pytest.skip("Redis is not reachable")

    try:
        yield streams
    finally:
        if streams.client is not None:
            await streams.client.delete(get_settings().redis_stream_name)
        await streams.close()
        get_settings.cache_clear()


def _event() -> SchedulingEvent:
    return SchedulingEvent(
        event_id=uuid4(),
        event_type="JOB_CREATED",
        job_id=uuid4(),
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_redis_connection_and_health_check(redis_streams: RedisStreams) -> None:
    assert redis_streams.client is not None
    assert await redis_streams.health_check() is True


@pytest.mark.asyncio
async def test_publish_read_and_deserialize_event(redis_streams: RedisStreams) -> None:
    await redis_streams.create_consumer_group()
    event = _event()

    message_id = await redis_streams.publish(event)
    messages = await redis_streams.read_group(consumer_name=f"consumer-{uuid4().hex}")

    assert isinstance(message_id, str)
    assert len(messages) == 1
    read_message_id, fields = messages[0]
    assert read_message_id == message_id
    assert fields == {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "job_id": str(event.job_id),
        "created_at": event.created_at.isoformat(),
    }
    assert RedisStreams.deserialize_event(fields) == event


@pytest.mark.asyncio
async def test_consumer_group_acknowledges_and_tracks_pending_messages(
    redis_streams: RedisStreams,
) -> None:
    await redis_streams.create_consumer_group()
    await redis_streams.create_consumer_group()
    consumer_name = f"consumer-{uuid4().hex}"

    acknowledged_event = _event()
    acknowledged_message_id = await redis_streams.publish(acknowledged_event)
    acknowledged_messages = await redis_streams.read_group(consumer_name=consumer_name)

    assert [message_id for message_id, _ in acknowledged_messages] == [acknowledged_message_id]
    assert await redis_streams.acknowledge(acknowledged_message_id) == 1
    assert acknowledged_message_id not in {
        message["message_id"] for message in await redis_streams.pending()
    }

    pending_event = _event()
    pending_message_id = await redis_streams.publish(pending_event)
    pending_messages = await redis_streams.read_group(consumer_name=consumer_name)

    assert [message_id for message_id, _ in pending_messages] == [pending_message_id]
    assert pending_message_id in {
        message["message_id"] for message in await redis_streams.pending()
    }
