import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from forge.db.config import get_settings
from forge.db.models import Job, OutboxEvent
from forge.db.session import AsyncSessionFactory
from forge.messaging.redis_streams import RedisStreams, SchedulingEvent
from forge.outbox.publisher import OutboxPublisher
from forge.outbox.service import add_outbox_event, scheduling_event_payload


@pytest.fixture(scope="module")
def migrated_database() -> Generator[None, None, None]:
    database_url = get_settings().async_database_url
    sync_url = database_url.replace("+psycopg", "")
    check = subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"sqlalchemy.url={sync_url}", "current"],
        check=False,
        capture_output=True,
        text=True,
    )
    if check.returncode != 0:
        pytest.skip("PostgreSQL is not reachable or Alembic is unavailable")

    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True)
    yield
    subprocess.run([sys.executable, "-m", "alembic", "downgrade", "base"], check=True)


@pytest_asyncio.fixture
async def session(migrated_database: None) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionFactory() as db_session:
        try:
            yield db_session
        finally:
            await db_session.rollback()
            await db_session.execute(delete(OutboxEvent))
            await db_session.execute(delete(Job))
            await db_session.commit()


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


def _job(job_id: UUID) -> Job:
    return Job(
        id=job_id,
        name=f"outbox-job-{job_id}",
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=80,
        cpu_required=1,
        memory_required_mb=128,
        gpu_required=0,
        required_capabilities=["python"],
        max_retries=3,
    )


def _scheduling_event(job_id: UUID) -> SchedulingEvent:
    return SchedulingEvent(
        event_id=uuid4(),
        event_type="JOB_CREATED",
        job_id=job_id,
        created_at=datetime.now(UTC),
    )


async def _create_outbox_event(
    session: AsyncSession,
    scheduling_event: SchedulingEvent,
    *,
    created_at: datetime | None = None,
    published_at: datetime | None = None,
) -> UUID:
    outbox_event = add_outbox_event(
        session,
        event_type=scheduling_event.event_type,
        payload=scheduling_event_payload(scheduling_event),
    )
    if created_at is not None:
        outbox_event.created_at = created_at
    outbox_event.published_at = published_at
    await session.commit()
    return outbox_event.id


async def _get_outbox_event(event_id: UUID) -> OutboxEvent | None:
    async with AsyncSessionFactory() as verification_session:
        return await verification_session.get(OutboxEvent, event_id)


@pytest.mark.asyncio
async def test_add_outbox_event_commits_with_business_record(session: AsyncSession) -> None:
    job_id = uuid4()
    scheduling_event = _scheduling_event(job_id)
    payload = scheduling_event_payload(scheduling_event)

    session.add(_job(job_id))
    outbox_event = add_outbox_event(
        session,
        event_type=scheduling_event.event_type,
        payload=payload,
    )
    await session.commit()

    async with AsyncSessionFactory() as verification_session:
        persisted_job = await verification_session.get(Job, job_id)
        persisted_event = await verification_session.get(OutboxEvent, outbox_event.id)

    assert persisted_job is not None
    assert persisted_event is not None
    assert persisted_event.event_type == "JOB_CREATED"
    assert persisted_event.payload == {
        "event_id": str(scheduling_event.event_id),
        "event_type": "JOB_CREATED",
        "job_id": str(job_id),
        "created_at": scheduling_event.created_at.isoformat(),
    }
    assert persisted_event.published_at is None
    assert persisted_event.retry_count == 0


@pytest.mark.asyncio
async def test_rollback_removes_business_record_and_outbox_event(session: AsyncSession) -> None:
    job_id = uuid4()
    scheduling_event = _scheduling_event(job_id)

    session.add(_job(job_id))
    outbox_event = add_outbox_event(
        session,
        event_type=scheduling_event.event_type,
        payload=scheduling_event_payload(scheduling_event),
    )
    await session.flush()
    outbox_event_id = outbox_event.id
    await session.rollback()

    async with AsyncSessionFactory() as verification_session:
        persisted_job = await verification_session.get(Job, job_id)
        persisted_event = await verification_session.get(OutboxEvent, outbox_event_id)

    assert persisted_job is None
    assert persisted_event is None


@pytest.mark.asyncio
async def test_add_outbox_event_does_not_publish_to_redis(
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream_name = f"forge:scheduling:test:{uuid4().hex}"
    monkeypatch.setenv("REDIS_STREAM_NAME", stream_name)
    get_settings.cache_clear()

    streams = RedisStreams()
    await streams.connect()
    try:
        if not await streams.health_check():
            pytest.skip("Redis is not reachable")

        job_id = uuid4()
        scheduling_event = _scheduling_event(job_id)
        session.add(_job(job_id))
        add_outbox_event(
            session,
            event_type=scheduling_event.event_type,
            payload=scheduling_event_payload(scheduling_event),
        )
        await session.commit()

        assert streams.client is not None
        assert await streams.client.exists(stream_name) == 0
    finally:
        if streams.client is not None:
            await streams.client.delete(stream_name)
        await streams.close()
        get_settings.cache_clear()


@pytest.mark.asyncio
async def test_get_pending_events_returns_unpublished_events_in_creation_order(
    session: AsyncSession,
    redis_streams: RedisStreams,
) -> None:
    publisher = OutboxPublisher(session, redis_streams)
    created_at = datetime(2026, 1, 1, tzinfo=UTC)
    first_event = _scheduling_event(uuid4())
    second_event = _scheduling_event(uuid4())
    published_event = _scheduling_event(uuid4())

    first_id = await _create_outbox_event(
        session,
        first_event,
        created_at=created_at,
    )
    second_id = await _create_outbox_event(
        session,
        second_event,
        created_at=created_at + timedelta(seconds=1),
    )
    await _create_outbox_event(
        session,
        published_event,
        created_at=created_at + timedelta(seconds=2),
        published_at=created_at + timedelta(seconds=3),
    )

    pending_events = await publisher.get_pending_events()

    assert [event.id for event in pending_events] == [first_id, second_id]


@pytest.mark.asyncio
async def test_outbox_payload_converts_to_scheduling_event(
    session: AsyncSession,
    redis_streams: RedisStreams,
) -> None:
    publisher = OutboxPublisher(session, redis_streams)
    scheduling_event = _scheduling_event(uuid4())
    outbox_event = OutboxEvent(
        event_type=scheduling_event.event_type,
        payload=scheduling_event_payload(scheduling_event),
    )

    assert publisher._to_scheduling_event(outbox_event) == scheduling_event


@pytest.mark.asyncio
async def test_publish_pending_publishes_event_and_persists_state(
    session: AsyncSession,
    redis_streams: RedisStreams,
) -> None:
    publisher = OutboxPublisher(session, redis_streams)
    scheduling_event = _scheduling_event(uuid4())
    outbox_event_id = await _create_outbox_event(session, scheduling_event)

    assert await publisher.publish_pending() == 1

    assert redis_streams.client is not None
    messages = await redis_streams.client.xrange(get_settings().redis_stream_name)
    assert len(messages) == 1
    assert messages[0][1] == scheduling_event_payload(scheduling_event)

    persisted_event = await _get_outbox_event(outbox_event_id)
    assert persisted_event is not None
    assert persisted_event.published_at is not None
    assert persisted_event.retry_count == 0
    assert persisted_event.last_error is None


@pytest.mark.asyncio
async def test_publish_pending_publishes_multiple_events(
    session: AsyncSession,
    redis_streams: RedisStreams,
) -> None:
    publisher = OutboxPublisher(session, redis_streams)
    first_id = await _create_outbox_event(session, _scheduling_event(uuid4()))
    second_id = await _create_outbox_event(session, _scheduling_event(uuid4()))

    assert await publisher.publish_pending() == 2

    assert redis_streams.client is not None
    assert len(await redis_streams.client.xrange(get_settings().redis_stream_name)) == 2
    for event_id in (first_id, second_id):
        persisted_event = await _get_outbox_event(event_id)
        assert persisted_event is not None
        assert persisted_event.published_at is not None


@pytest.mark.asyncio
async def test_publish_failure_records_error_without_publishing(
    session: AsyncSession,
    redis_streams: RedisStreams,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = OutboxPublisher(session, redis_streams)
    outbox_event_id = await _create_outbox_event(session, _scheduling_event(uuid4()))
    failure = RuntimeError("Redis publish failed")
    monkeypatch.setattr(redis_streams, "publish", AsyncMock(side_effect=failure))

    assert await publisher.publish_pending() == 0

    persisted_event = await _get_outbox_event(outbox_event_id)
    assert persisted_event is not None
    assert persisted_event.published_at is None
    assert persisted_event.retry_count == 1
    assert "Redis publish failed" in persisted_event.last_error


@pytest.mark.asyncio
async def test_failed_event_does_not_stop_later_events(
    session: AsyncSession,
    redis_streams: RedisStreams,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = OutboxPublisher(session, redis_streams)
    created_at = datetime(2026, 1, 1, tzinfo=UTC)
    failed_id = await _create_outbox_event(
        session,
        _scheduling_event(uuid4()),
        created_at=created_at,
    )
    successful_id = await _create_outbox_event(
        session,
        _scheduling_event(uuid4()),
        created_at=created_at + timedelta(seconds=1),
    )
    monkeypatch.setattr(
        redis_streams,
        "publish",
        AsyncMock(side_effect=[RuntimeError("first event failed"), "1-0"]),
    )

    assert await publisher.publish_pending() == 1

    failed_event = await _get_outbox_event(failed_id)
    successful_event = await _get_outbox_event(successful_id)
    assert failed_event is not None
    assert successful_event is not None
    assert failed_event.published_at is None
    assert failed_event.retry_count == 1
    assert "first event failed" in failed_event.last_error
    assert successful_event.published_at is not None
    assert successful_event.retry_count == 0


@pytest.mark.asyncio
async def test_failed_event_can_be_published_on_a_later_attempt(
    session: AsyncSession,
    redis_streams: RedisStreams,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = OutboxPublisher(session, redis_streams)
    scheduling_event = _scheduling_event(uuid4())
    outbox_event_id = await _create_outbox_event(session, scheduling_event)
    original_publish = redis_streams.publish
    monkeypatch.setattr(
        redis_streams,
        "publish",
        AsyncMock(side_effect=RuntimeError("temporary Redis failure")),
    )

    assert await publisher.publish_pending() == 0
    failed_event = await _get_outbox_event(outbox_event_id)
    assert failed_event is not None
    assert failed_event.retry_count == 1
    assert failed_event.published_at is None

    monkeypatch.setattr(redis_streams, "publish", original_publish)

    assert await publisher.publish_pending() == 1

    persisted_event = await _get_outbox_event(outbox_event_id)
    assert persisted_event is not None
    assert persisted_event.retry_count == 1
    assert persisted_event.published_at is not None
    assert redis_streams.client is not None
    assert (await redis_streams.client.xrange(get_settings().redis_stream_name))[0][1] == (
        scheduling_event_payload(scheduling_event)
    )
