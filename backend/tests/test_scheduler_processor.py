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
    scheduler.schedule_next_job = AsyncMock()
    return scheduler


@pytest.fixture
def processor(mock_scheduler: AsyncMock) -> SchedulerEventProcessor:
    return SchedulerEventProcessor(scheduler=mock_scheduler)


# ---------------------------------------------------------------------------
# 1. SchedulerEventProcessor unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_processor_handles_job_created_with_valid_job_id(
    processor: SchedulerEventProcessor,
    mock_scheduler: AsyncMock,
) -> None:
    job_id = str(uuid4())
    assignment = Mock(spec=Assignment, id=uuid4(), status=AssignmentStatus.CREATED)
    mock_scheduler.schedule_next_job.return_value = assignment

    data = {
        "event_type": "JOB_CREATED",
        "job_id": job_id,
    }

    await processor.process("1700000000000-0", data)

    mock_scheduler.schedule_next_job.assert_awaited_once()


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

    mock_scheduler.schedule_next_job.assert_not_called()


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

    mock_scheduler.schedule_next_job.assert_not_called()


@pytest.mark.asyncio
async def test_processor_surfaces_scheduling_failure_when_schedule_next_job_returns_none(
    processor: SchedulerEventProcessor,
    mock_scheduler: AsyncMock,
) -> None:
    job_id = str(uuid4())
    mock_scheduler.schedule_next_job.return_value = None

    data = {
        "event_type": "JOB_CREATED",
        "job_id": job_id,
    }

    with pytest.raises(
        RuntimeError,
        match=f"Unable to schedule JOB_CREATED event for job {job_id}",
    ):
        await processor.process("1700000000000-0", data)

    mock_scheduler.schedule_next_job.assert_awaited_once()


# ---------------------------------------------------------------------------
# 2. Redis Event Consumer & Processor Integration
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
    mock_scheduler.schedule_next_job.return_value = assignment

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
    )
    processor = SchedulerEventProcessor(scheduler=mock_scheduler)

    # Consumer reads new message
    messages = await consumer.consume(count=1)
    assert len(messages) == 1
    msg_id, data = messages[0]

    # Processor processes the message
    await processor.process(msg_id, data)
    mock_scheduler.schedule_next_job.assert_awaited_once()

    # On success, message is acknowledged
    await consumer.acknowledge(msg_id)
    mock_redis.xack.assert_awaited_once_with(
        "events:jobs",
        "scheduler-group",
        message_id,
    )


@pytest.mark.asyncio
async def test_consumer_and_processor_flow_failed_scheduling_does_not_acknowledge() -> None:
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
    mock_scheduler.schedule_next_job.return_value = None  # No ready workers available

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
    )
    processor = SchedulerEventProcessor(scheduler=mock_scheduler)

    messages = await consumer.consume(count=1)
    assert len(messages) == 1
    msg_id, data = messages[0]

    # Processing fails with RuntimeError
    with pytest.raises(RuntimeError, match="Unable to schedule JOB_CREATED event"):
        await processor.process(msg_id, data)

    # Message is NOT acknowledged so it remains pending in Redis for later retry
    mock_redis.xack.assert_not_called()
