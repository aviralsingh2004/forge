import asyncio
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from redis import asyncio as aioredis
from redis.asyncio import Redis

from forge.db.config import get_settings
from forge.scheduler.consumer import SchedulerEventConsumer


# ---------------------------------------------------------------------------
# Unit Tests (Mock Redis)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_redis() -> AsyncMock:
    redis = AsyncMock()
    redis.xreadgroup = AsyncMock()
    redis.xack = AsyncMock()
    redis.xautoclaim = AsyncMock()
    return redis


@pytest.fixture
def consumer(mock_redis: AsyncMock) -> SchedulerEventConsumer:
    return SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
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


# ---------------------------------------------------------------------------
# Unit Tests for consume_stale_pending()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consume_stale_pending_calls_xautoclaim_with_expected_arguments(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xautoclaim.return_value = ("0-0", [], [])

    await consumer.consume_stale_pending(count=5)

    mock_redis.xautoclaim.assert_awaited_once_with(
        name="events:jobs",
        groupname="scheduler-group",
        consumername="scheduler-worker-1",
        min_idle_time=60_000,
        start_id="0-0",
        count=5,
    )


@pytest.mark.asyncio
async def test_consume_stale_pending_uses_default_count(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xautoclaim.return_value = ("0-0", [], [])

    await consumer.consume_stale_pending()

    mock_redis.xautoclaim.assert_awaited_once_with(
        name="events:jobs",
        groupname="scheduler-group",
        consumername="scheduler-worker-1",
        min_idle_time=60_000,
        start_id="0-0",
        count=1,
    )


@pytest.mark.asyncio
async def test_consume_stale_pending_parses_claimed_messages(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xautoclaim.return_value = (
        "0-0",
        [
            ("1700000000001-0", {"job_id": "job-stale-1", "event_type": "JOB_CREATED"}),
            ("1700000000002-0", {"job_id": "job-stale-2", "event_type": "JOB_CREATED"}),
        ],
        [],
    )

    events = await consumer.consume_stale_pending(count=5)

    assert events == [
        ("1700000000001-0", {"job_id": "job-stale-1", "event_type": "JOB_CREATED"}),
        ("1700000000002-0", {"job_id": "job-stale-2", "event_type": "JOB_CREATED"}),
    ]


@pytest.mark.asyncio
async def test_consume_stale_pending_returns_empty_list_when_no_stale_messages(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xautoclaim.return_value = ("0-0", [], [])

    events = await consumer.consume_stale_pending()

    assert events == []


@pytest.mark.asyncio
async def test_consume_stale_pending_propagates_redis_exception(
    consumer: SchedulerEventConsumer,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.xautoclaim.side_effect = RuntimeError("Redis XAUTOCLAIM failure")

    with pytest.raises(RuntimeError, match="Redis XAUTOCLAIM failure"):
        await consumer.consume_stale_pending()


# ---------------------------------------------------------------------------
# Integration Tests with Real Redis (Section B Requirements 1-8)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def real_redis_client() -> AsyncGenerator[Redis, None]:
    settings = get_settings()
    client: Redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("Redis is not reachable")

    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def redis_stream_and_group(
    real_redis_client: Redis,
) -> AsyncGenerator[tuple[str, str], None]:
    stream_name = f"forge:test:stream:{uuid4().hex}"
    group_name = f"forge:test:group:{uuid4().hex}"

    await real_redis_client.xgroup_create(
        name=stream_name,
        groupname=group_name,
        id="0",
        mkstream=True,
    )

    try:
        yield stream_name, group_name
    finally:
        await real_redis_client.delete(stream_name)


@pytest.mark.asyncio
async def test_consume_stale_pending_not_claimed_when_idle_time_not_met(
    real_redis_client: Redis,
    redis_stream_and_group: tuple[str, str],
) -> None:
    """Requirement B1: Pending message owned by another consumer idle for < min_idle is NOT claimed."""
    stream_name, group_name = redis_stream_and_group

    consumer_a = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-A",
        pending_min_idle_ms=60_000,
    )
    consumer_b = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-B",
        pending_min_idle_ms=60_000,
    )

    msg_id = await real_redis_client.xadd(stream_name, {"job_id": "job-1", "event_type": "JOB_CREATED"})
    # Consumer A reads it, placing it into Consumer A's pending list
    messages = await consumer_a.consume(count=1)
    assert len(messages) == 1
    assert messages[0][0] == msg_id

    # Consumer B immediately tries to claim with min_idle=60,000ms -> should not be claimed
    stale = await consumer_b.consume_stale_pending(count=1)
    assert stale == []


@pytest.mark.asyncio
async def test_consume_stale_pending_claimed_when_idle_time_met(
    real_redis_client: Redis,
    redis_stream_and_group: tuple[str, str],
) -> None:
    """Requirements B2, B3, B4: Pending message idle for >= min_idle is claimed, owned by recovering consumer, returned in expected format."""
    stream_name, group_name = redis_stream_and_group

    consumer_a = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-A",
        pending_min_idle_ms=50,
    )
    consumer_b = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-B",
        pending_min_idle_ms=50,
    )

    msg_id = await real_redis_client.xadd(stream_name, {"job_id": "job-2", "event_type": "JOB_CREATED"})
    # Consumer A reads it
    await consumer_a.consume(count=1)

    # Wait for the message to become stale (> 50ms)
    await asyncio.sleep(0.06)

    # Consumer B claims it
    stale = await consumer_b.consume_stale_pending(count=1)

    # Requirement B4: formatted as list[tuple[str, dict[str, str]]]
    assert len(stale) == 1
    assert stale[0][0] == msg_id
    assert stale[0][1] == {"job_id": "job-2", "event_type": "JOB_CREATED"}

    # Requirement B3: After claiming, message is owned by Consumer B
    pending_info = await real_redis_client.xpending_range(
        name=stream_name,
        groupname=group_name,
        min="-",
        max="+",
        count=10,
    )
    assert len(pending_info) == 1
    assert pending_info[0]["message_id"] == msg_id
    assert pending_info[0]["consumer"] == "consumer-B"


@pytest.mark.asyncio
async def test_consume_stale_pending_multiple_messages_with_count(
    real_redis_client: Redis,
    redis_stream_and_group: tuple[str, str],
) -> None:
    """Requirement B5: Multiple stale pending messages can be claimed when count permits."""
    stream_name, group_name = redis_stream_and_group

    consumer_a = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-A",
        pending_min_idle_ms=50,
    )
    consumer_b = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-B",
        pending_min_idle_ms=50,
    )

    msg_id_1 = await real_redis_client.xadd(stream_name, {"job_id": "job-1", "event_type": "JOB_CREATED"})
    msg_id_2 = await real_redis_client.xadd(stream_name, {"job_id": "job-2", "event_type": "JOB_CREATED"})
    msg_id_3 = await real_redis_client.xadd(stream_name, {"job_id": "job-3", "event_type": "JOB_CREATED"})

    # Consumer A reads all 3
    messages = await consumer_a.consume(count=3)
    assert len(messages) == 3

    # Wait until stale
    await asyncio.sleep(0.06)

    # Claim with count=2
    stale_batch_1 = await consumer_b.consume_stale_pending(count=2)
    assert len(stale_batch_1) == 2
    assert [m[0] for m in stale_batch_1] == [msg_id_1, msg_id_2]

    # Claim remaining with count=2
    stale_batch_2 = await consumer_b.consume_stale_pending(count=2)
    assert len(stale_batch_2) == 1
    assert stale_batch_2[0][0] == msg_id_3


@pytest.mark.asyncio
async def test_consume_stale_pending_does_not_return_acked_messages(
    real_redis_client: Redis,
    redis_stream_and_group: tuple[str, str],
) -> None:
    """Requirement B6: An already ACKed message is not returned by consume_stale_pending()."""
    stream_name, group_name = redis_stream_and_group

    consumer_a = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-A",
        pending_min_idle_ms=50,
    )
    consumer_b = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-B",
        pending_min_idle_ms=50,
    )

    msg_id = await real_redis_client.xadd(stream_name, {"job_id": "job-1", "event_type": "JOB_CREATED"})
    await consumer_a.consume(count=1)
    await consumer_a.acknowledge(msg_id)

    await asyncio.sleep(0.06)

    stale = await consumer_b.consume_stale_pending(count=10)
    assert stale == []


@pytest.mark.asyncio
async def test_consume_pending_and_consume_new_remain_functional(
    real_redis_client: Redis,
    redis_stream_and_group: tuple[str, str],
) -> None:
    """Requirements B7, B8: consume_pending reads own pending, and consume reads new with >."""
    stream_name, group_name = redis_stream_and_group

    consumer_a = SchedulerEventConsumer(
        redis=real_redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        consumer_name="consumer-A",
        pending_min_idle_ms=60_000,
    )

    msg_id_1 = await real_redis_client.xadd(stream_name, {"job_id": "job-1", "event_type": "JOB_CREATED"})

    # Requirement B8: consume reads new messages with >
    new_messages = await consumer_a.consume(count=1)
    assert len(new_messages) == 1
    assert new_messages[0][0] == msg_id_1

    # Requirement B7: consume_pending reads own pending messages with ID 0
    pending_messages = await consumer_a.consume_pending(count=1)
    assert len(pending_messages) == 1
    assert pending_messages[0][0] == msg_id_1
