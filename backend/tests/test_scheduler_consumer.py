from unittest.mock import AsyncMock

import pytest

from forge.scheduler.consumer import SchedulerEventConsumer


@pytest.fixture
def mock_redis() -> AsyncMock:
    redis = AsyncMock()
    redis.xreadgroup = AsyncMock()
    redis.xack = AsyncMock()
    return redis


@pytest.fixture
def consumer(mock_redis: AsyncMock) -> SchedulerEventConsumer:
    return SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
    )


@pytest.mark.asyncio
async def test_consume_calls_xreadgroup_with_expected_arguments(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = []

    await consumer.consume(count=5, block_ms=2500)

    mock_redis.xreadgroup.assert_awaited_once_with(
        groupname="scheduler-group",
        consumername="scheduler-worker-1",
        streams={"events:jobs": ">"},
        count=5,
        block=2500,
    )


@pytest.mark.asyncio
async def test_consume_uses_default_count_and_block_ms(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = []

    await consumer.consume()

    mock_redis.xreadgroup.assert_awaited_once_with(
        groupname="scheduler-group",
        consumername="scheduler-worker-1",
        streams={"events:jobs": ">"},
        count=1,
        block=1000,
    )


@pytest.mark.asyncio
async def test_consume_converts_single_message(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = [
        ("events:jobs", [("1700000000000-0", {"job_id": "job-123", "event": "JOB_CREATED"})])
    ]

    events = await consumer.consume()

    assert events == [
        ("1700000000000-0", {"job_id": "job-123", "event": "JOB_CREATED"}),
    ]


@pytest.mark.asyncio
async def test_consume_flattens_multiple_messages_in_single_stream(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = [
        (
            "events:jobs",
            [
                ("1700000000000-0", {"job_id": "job-1", "event": "JOB_CREATED"}),
                ("1700000000001-0", {"job_id": "job-2", "event": "JOB_CREATED"}),
                ("1700000000002-0", {"job_id": "job-3", "event": "JOB_CREATED"}),
            ],
        )
    ]

    events = await consumer.consume(count=10)

    assert events == [
        ("1700000000000-0", {"job_id": "job-1", "event": "JOB_CREATED"}),
        ("1700000000001-0", {"job_id": "job-2", "event": "JOB_CREATED"}),
        ("1700000000002-0", {"job_id": "job-3", "event": "JOB_CREATED"}),
    ]


@pytest.mark.asyncio
async def test_consume_handles_multiple_stream_entries(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = [
        (
            "events:jobs",
            [
                ("1700000000000-0", {"job_id": "job-1", "event": "JOB_CREATED"}),
            ],
        ),
        (
            "events:workers",
            [
                ("1700000000001-0", {"worker_id": "worker-1", "event": "WORKER_READY"}),
                ("1700000000002-0", {"worker_id": "worker-2", "event": "WORKER_READY"}),
            ],
        ),
    ]

    events = await consumer.consume(count=10)

    assert events == [
        ("1700000000000-0", {"job_id": "job-1", "event": "JOB_CREATED"}),
        ("1700000000001-0", {"worker_id": "worker-1", "event": "WORKER_READY"}),
        ("1700000000002-0", {"worker_id": "worker-2", "event": "WORKER_READY"}),
    ]


@pytest.mark.asyncio
async def test_consume_returns_empty_list_when_no_messages(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = []

    events = await consumer.consume()

    assert events == []


@pytest.mark.asyncio
async def test_consume_returns_empty_list_when_stream_has_empty_messages(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = [
        ("events:jobs", []),
    ]

    events = await consumer.consume()

    assert events == []


@pytest.mark.asyncio
async def test_acknowledge_calls_xack_exactly_once_with_configured_arguments(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    message_id = "1700000000000-0"

    await consumer.acknowledge(message_id)

    mock_redis.xack.assert_awaited_once_with(
        "events:jobs",
        "scheduler-group",
        "1700000000000-0",
    )


@pytest.mark.asyncio
async def test_acknowledge_propagates_redis_exception(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xack.side_effect = RuntimeError("Redis connection failure")

    with pytest.raises(RuntimeError, match="Redis connection failure"):
        await consumer.acknowledge("1700000000000-0")


@pytest.mark.asyncio
async def test_consume_pending_calls_xreadgroup_with_stream_id_zero(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = []

    await consumer.consume_pending(count=3)

    mock_redis.xreadgroup.assert_awaited_once_with(
        groupname="scheduler-group",
        consumername="scheduler-worker-1",
        streams={"events:jobs": "0"},
        count=3,
        block=0,
    )


@pytest.mark.asyncio
async def test_consume_pending_uses_default_count(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = []

    await consumer.consume_pending()

    mock_redis.xreadgroup.assert_awaited_once_with(
        groupname="scheduler-group",
        consumername="scheduler-worker-1",
        streams={"events:jobs": "0"},
        count=1,
        block=0,
    )


@pytest.mark.asyncio
async def test_consume_pending_parses_messages_correctly(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = [
        (
            "events:jobs",
            [
                ("1700000000001-0", {"job_id": "job-pending-1", "event_type": "JOB_CREATED"}),
                ("1700000000002-0", {"job_id": "job-pending-2", "event_type": "JOB_CREATED"}),
            ],
        )
    ]

    events = await consumer.consume_pending(count=5)

    assert events == [
        ("1700000000001-0", {"job_id": "job-pending-1", "event_type": "JOB_CREATED"}),
        ("1700000000002-0", {"job_id": "job-pending-2", "event_type": "JOB_CREATED"}),
    ]


@pytest.mark.asyncio
async def test_consume_pending_returns_empty_list_when_no_pending_messages(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xreadgroup.return_value = []

    events = await consumer.consume_pending()

    assert events == []
