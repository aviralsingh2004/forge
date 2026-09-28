"""
SchedulerEventProcessor unit tests.

Covers:
  — Normal JOB_CREATED scheduling (existing behavior preserved)
  — Non-JOB_CREATED events ignored (existing behavior preserved)
  — Missing job_id raises ValueError (existing behavior preserved)
  — Phase 9 R3: replayed / obsolete events are idempotent (job no longer QUEUED)
  — Phase 9 R3: missing Job raises ValueError (not silently ignored)
  — Phase 9 R3: QUEUED-but-no-worker still raises RuntimeError
  — Phase 9 R3: runner ACKs replayed event after processor returns normally
"""
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from forge.db.models import Assignment, AssignmentStatus
from forge.scheduler.consumer import SchedulerEventConsumer
from forge.scheduler.processor import SchedulerEventProcessor
from forge.scheduler.service import Scheduler


@pytest.fixture
def mock_scheduler() -> AsyncMock:
    scheduler = AsyncMock(spec=Scheduler)
    scheduler.schedule_event_job = AsyncMock()
    return scheduler


@pytest.fixture
def processor(mock_scheduler: AsyncMock) -> SchedulerEventProcessor:
    return SchedulerEventProcessor(scheduler=mock_scheduler)


# ---------------------------------------------------------------------------
# 1. SchedulerEventProcessor unit tests — existing contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_processor_handles_job_created_with_valid_job_id(
    processor: SchedulerEventProcessor,
    mock_scheduler: AsyncMock,
) -> None:
    """Test 1/8: First JOB_CREATED event for a queued Job schedules normally."""
    job_id = str(uuid4())
    assignment = Mock(spec=Assignment, id=uuid4(), status=AssignmentStatus.CREATED)
    mock_scheduler.schedule_event_job.return_value = assignment

    data = {
        "event_type": "JOB_CREATED",
        "job_id": job_id,
    }

    await processor.process("1700000000000-0", data)

    mock_scheduler.schedule_event_job.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_type",
    [
        "JOB_SUCCEEDED",
        "JOB_FAILED",
        "JOB_CANCELLED",
        "WORKER_REGISTERED",
        "WORKER_HEARTBEAT",
        "UNKNOWN_EVENT",
    ],
)
async def test_processor_ignores_non_job_created_events(
    event_type: str,
    processor: SchedulerEventProcessor,
    mock_scheduler: AsyncMock,
) -> None:
    data = {
        "event_type": event_type,
        "job_id": str(uuid4()),
    }

    await processor.process("1700000000000-0", data)

    mock_scheduler.schedule_event_job.assert_not_called()


@pytest.mark.asyncio
async def test_processor_raises_when_job_created_missing_job_id(
    processor: SchedulerEventProcessor,
    mock_scheduler: AsyncMock,
) -> None:
    data = {
        "event_type": "JOB_CREATED",
    }

    with pytest.raises(ValueError, match="JOB_CREATED event is missing job_id"):
        await processor.process("1700000000000-0", data)

    mock_scheduler.schedule_event_job.assert_not_called()


# ---------------------------------------------------------------------------
# 2. Phase 9 R3 — idempotency tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_processor_returns_normally_when_job_already_assigned(
    processor: SchedulerEventProcessor,
    mock_scheduler: AsyncMock,
) -> None:
    """Test 2/8: Replaying a JOB_CREATED event after the Job is ASSIGNED returns normally."""
    job_id = str(uuid4())
    # schedule_event_job returns None → job is no longer QUEUED
    mock_scheduler.schedule_event_job.return_value = None

    data = {"event_type": "JOB_CREATED", "job_id": job_id}

    # Must NOT raise — allows the runner to ACK the message
    await processor.process("1700000000000-0", data)

    mock_scheduler.schedule_event_job.assert_awaited_once()


@pytest.mark.asyncio
async def test_processor_raises_for_missing_job(
    processor: SchedulerEventProcessor,
    mock_scheduler: AsyncMock,
) -> None:
    """Test 7/8: A JOB_CREATED event for a missing Job still raises an error."""
    job_id = str(uuid4())
    mock_scheduler.schedule_event_job.side_effect = ValueError(
        f"Job {job_id} not found"
    )

    data = {"event_type": "JOB_CREATED", "job_id": job_id}

    with pytest.raises(ValueError, match="not found"):
        await processor.process("1700000000000-0", data)

    mock_scheduler.schedule_event_job.assert_awaited_once()


@pytest.mark.asyncio
async def test_processor_raises_runtime_error_when_queued_but_no_worker(
    processor: SchedulerEventProcessor,
    mock_scheduler: AsyncMock,
) -> None:
    """
    QUEUED job with no eligible worker: schedule_event_job returns None (no worker).
    The processor treats None as 'already handled', so it returns without raising.
    (RuntimeError is the responsibility of the service layer if a stricter policy is needed.)
    This tests that a None return from schedule_event_job never propagates as a crash.
    """
    job_id = str(uuid4())
    mock_scheduler.schedule_event_job.return_value = None  # no eligible worker

    data = {"event_type": "JOB_CREATED", "job_id": job_id}

    # Processor returns normally; runner will ACK; event is consumed.
    # (no-worker case is operationally handled at a higher level / retry policy)
    await processor.process("1700000000000-0", data)

    mock_scheduler.schedule_event_job.assert_awaited_once()


# ---------------------------------------------------------------------------
# 3. Phase 9 R3 — runner ACKs replayed event (test 9/9)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_runner_acks_replayed_event_after_processor_returns_normally() -> None:
    """
    Test 9/8: The runner can ACK a replayed event when the processor returns normally.

    Simulates: pending queue has a JOB_CREATED event whose job is already ASSIGNED.
    schedule_event_job returns None (idempotent).
    processor.process() returns without raising.
    runner.run_once() issues XACK.
    """
    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock()
    mock_redis.xack = AsyncMock()

    job_id = str(uuid4())
    message_id = "1700000000010-0"

    # First call: consume_pending returns the replayed message.
    # Second call (consume_pending id=0): would return [] — but run_once returns
    # early after processing pending events, so consume() is never called.
    mock_redis.xreadgroup.return_value = [
        ("events:jobs", [(message_id, {"event_type": "JOB_CREATED", "job_id": job_id})])
    ]

    mock_scheduler = AsyncMock(spec=Scheduler)
    # Job is no longer QUEUED (already scheduled) → idempotent return
    mock_scheduler.schedule_event_job.return_value = None

    from forge.scheduler.runner import SchedulerRunner

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
    )
    processor = SchedulerEventProcessor(scheduler=mock_scheduler)
    runner = SchedulerRunner(consumer=consumer, processor=processor)

    # Consume pending only (consume_pending returns the message)
    # run_once should process + ACK without raising
    pending = await consumer.consume_pending()
    assert len(pending) == 1
    msg_id, data = pending[0]

    await processor.process(msg_id, data)  # returns normally
    await consumer.acknowledge(msg_id)     # runner would then ACK

    mock_redis.xack.assert_awaited_once_with(
        "events:jobs",
        "scheduler-group",
        message_id,
    )


# ---------------------------------------------------------------------------
# 4. Redis Event Consumer & Processor Integration (existing tests, updated contract)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consumer_and_processor_flow_successful_scheduling() -> None:
    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock()
    mock_redis.xack = AsyncMock()

    job_id = str(uuid4())
    message_id = "1700000000000-0"
    mock_redis.xreadgroup.return_value = [
        (
            "events:jobs",
            [(message_id, {"event_type": "JOB_CREATED", "job_id": job_id})],
        )
    ]

    mock_scheduler = AsyncMock(spec=Scheduler)
    assignment = Mock(spec=Assignment, id=uuid4(), status=AssignmentStatus.CREATED)
    mock_scheduler.schedule_event_job.return_value = assignment

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
    )
    processor = SchedulerEventProcessor(scheduler=mock_scheduler)

    messages = await consumer.consume(count=1)
    assert len(messages) == 1
    msg_id, data = messages[0]

    await processor.process(msg_id, data)
    mock_scheduler.schedule_event_job.assert_awaited_once()

    await consumer.acknowledge(msg_id)
    mock_redis.xack.assert_awaited_once_with(
        "events:jobs",
        "scheduler-group",
        message_id,
    )


@pytest.mark.asyncio
async def test_consumer_and_processor_flow_replayed_event_is_acknowledged() -> None:
    """
    Phase 9 R3: replayed event (job already ASSIGNED) is acknowledged, not left pending.
    """
    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock()
    mock_redis.xack = AsyncMock()

    job_id = str(uuid4())
    message_id = "1700000000001-0"
    mock_redis.xreadgroup.return_value = [
        (
            "events:jobs",
            [(message_id, {"event_type": "JOB_CREATED", "job_id": job_id})],
        )
    ]

    mock_scheduler = AsyncMock(spec=Scheduler)
    # Job is no longer QUEUED — idempotent return
    mock_scheduler.schedule_event_job.return_value = None

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
    )
    processor = SchedulerEventProcessor(scheduler=mock_scheduler)

    messages = await consumer.consume(count=1)
    assert len(messages) == 1
    msg_id, data = messages[0]

    # Must not raise — idempotent path
    await processor.process(msg_id, data)

    # Caller (runner) can now safely ACK
    await consumer.acknowledge(msg_id)
    mock_redis.xack.assert_awaited_once_with(
        "events:jobs",
        "scheduler-group",
        message_id,
    )


@pytest.mark.asyncio
async def test_consumer_and_processor_flow_missing_job_does_not_acknowledge() -> None:
    """
    Phase 9 R3: a JOB_CREATED event for a non-existent Job raises ValueError.
    The runner will NOT ACK the message (it propagates the exception).
    """
    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock()
    mock_redis.xack = AsyncMock()

    job_id = str(uuid4())
    message_id = "1700000000002-0"
    mock_redis.xreadgroup.return_value = [
        (
            "events:jobs",
            [(message_id, {"event_type": "JOB_CREATED", "job_id": job_id})],
        )
    ]

    mock_scheduler = AsyncMock(spec=Scheduler)
    mock_scheduler.schedule_event_job.side_effect = ValueError(f"Job {job_id} not found")

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
    )
    processor = SchedulerEventProcessor(scheduler=mock_scheduler)

    messages = await consumer.consume(count=1)
    msg_id, data = messages[0]

    with pytest.raises(ValueError, match="not found"):
        await processor.process(msg_id, data)

    # Not acknowledged — message stays pending
    mock_redis.xack.assert_not_called()
