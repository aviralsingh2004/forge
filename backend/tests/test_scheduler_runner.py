import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from forge.db.config import get_settings
from forge.db.models import (
    Assignment,
    AssignmentStatus,
    Attempt,
    AttemptStatus,
    Job,
    JobStatus,
    Reservation,
    ReservationStatus,
    Worker,
    WorkerStatus,
)
from forge.scheduler.consumer import SchedulerEventConsumer
from forge.scheduler.processor import SchedulerEventProcessor
from forge.scheduler.runner import SchedulerRunner
from forge.scheduler.service import Scheduler


def _database_url() -> str:
    return get_settings().async_database_url


@pytest.fixture(scope="module")
def migrated_database() -> Generator[str, None, None]:
    url = _database_url()
    sync_url = url.replace("+psycopg", "")
    check = subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"sqlalchemy.url={sync_url}", "current"],
        check=False,
        capture_output=True,
        text=True,
    )
    if check.returncode != 0:
        pytest.skip("PostgreSQL is not reachable or Alembic is unavailable")

    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True)
    yield url
    subprocess.run([sys.executable, "-m", "alembic", "downgrade", "base"], check=True)


@pytest_asyncio.fixture
async def session(migrated_database: str) -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(migrated_database)
    async with async_sessionmaker(engine, expire_on_commit=False)() as db_session:
        yield db_session
    await engine.dispose()


@pytest_asyncio.fixture
async def clean_database(session: AsyncSession) -> AsyncGenerator[None, None]:
    await session.execute(delete(Assignment))
    await session.execute(delete(Reservation))
    await session.execute(delete(Attempt))
    await session.execute(delete(Job))
    await session.execute(delete(Worker))
    await session.commit()
    yield
    await session.execute(delete(Assignment))
    await session.execute(delete(Reservation))
    await session.execute(delete(Attempt))
    await session.execute(delete(Job))
    await session.execute(delete(Worker))
    await session.commit()


@pytest.fixture
def mock_consumer() -> AsyncMock:
    consumer = AsyncMock(spec=SchedulerEventConsumer)
    consumer.consume_pending = AsyncMock(return_value=[])
    consumer.consume = AsyncMock(return_value=[])
    consumer.acknowledge = AsyncMock()
    return consumer


@pytest.fixture
def mock_processor() -> AsyncMock:
    processor = AsyncMock(spec=SchedulerEventProcessor)
    processor.process = AsyncMock()
    return processor


@pytest.fixture
def runner(
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> SchedulerRunner:
    return SchedulerRunner(
        consumer=mock_consumer,
        processor=mock_processor,
    )


# ---------------------------------------------------------------------------
# 1. SchedulerRunner.run_once() Unit Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_once_processes_pending_messages_before_new(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    pending_msg = ("1700000000001-0", {"event_type": "JOB_CREATED", "job_id": "job-1"})
    mock_consumer.consume_pending.return_value = [pending_msg]

    await runner.run_once()

    # Pending message is processed and acknowledged
    mock_processor.process.assert_awaited_once_with("1700000000001-0", pending_msg[1])
    mock_consumer.acknowledge.assert_awaited_once_with("1700000000001-0")

    # When pending messages exist, new messages are NOT consumed in the same iteration
    mock_consumer.consume.assert_not_called()


@pytest.mark.asyncio
async def test_run_once_processes_new_messages_when_no_pending(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    mock_consumer.consume_pending.return_value = []
    new_msg = ("1700000000002-0", {"event_type": "JOB_CREATED", "job_id": "job-2"})
    mock_consumer.consume.return_value = [new_msg]

    await runner.run_once()

    mock_consumer.consume_pending.assert_awaited_once()
    mock_consumer.consume.assert_awaited_once()
    mock_processor.process.assert_awaited_once_with("1700000000002-0", new_msg[1])
    mock_consumer.acknowledge.assert_awaited_once_with("1700000000002-0")


@pytest.mark.asyncio
async def test_run_once_does_not_acknowledge_when_processing_fails(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    mock_consumer.consume_pending.return_value = []
    msg = ("1700000000003-0", {"event_type": "JOB_CREATED", "job_id": "job-fail"})
    mock_consumer.consume.return_value = [msg]

    mock_processor.process.side_effect = RuntimeError("Scheduling failed")

    with pytest.raises(RuntimeError, match="Scheduling failed"):
        await runner.run_once()

    mock_processor.process.assert_awaited_once_with("1700000000003-0", msg[1])
    mock_consumer.acknowledge.assert_not_called()


@pytest.mark.asyncio
async def test_run_once_does_not_acknowledge_when_pending_processing_fails(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    pending_msg = ("1700000000004-0", {"event_type": "JOB_CREATED", "job_id": "job-pending-fail"})
    mock_consumer.consume_pending.return_value = [pending_msg]

    mock_processor.process.side_effect = RuntimeError("Pending processing error")

    with pytest.raises(RuntimeError, match="Pending processing error"):
        await runner.run_once()

    mock_processor.process.assert_awaited_once_with("1700000000004-0", pending_msg[1])
    mock_consumer.acknowledge.assert_not_called()
    mock_consumer.consume.assert_not_called()


@pytest.mark.asyncio
async def test_run_once_processes_multiple_messages_and_acknowledges_each(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    mock_consumer.consume_pending.return_value = []
    messages = [
        ("msg-1", {"event_type": "JOB_CREATED", "job_id": "job-1"}),
        ("msg-2", {"event_type": "JOB_CREATED", "job_id": "job-2"}),
    ]
    mock_consumer.consume.return_value = messages

    await runner.run_once()

    assert mock_processor.process.await_count == 2
    assert mock_consumer.acknowledge.await_count == 2
    mock_consumer.acknowledge.assert_any_await("msg-1")
    mock_consumer.acknowledge.assert_any_await("msg-2")


# ---------------------------------------------------------------------------
# 2. SchedulerRunner.run() and stop() Lifecycle Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_executes_repeatedly_and_stop_terminates_loop(
    runner: SchedulerRunner,
) -> None:
    call_count = 0

    async def _mock_run_once() -> None:
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            runner.stop()

    runner.run_once = _mock_run_once

    await runner.run()

    assert call_count == 3
    assert runner._stop_event.is_set()


@pytest.mark.asyncio
async def test_stop_prevents_run_from_executing_when_already_stopped(
    runner: SchedulerRunner,
) -> None:
    runner.stop()
    runner.run_once = AsyncMock()

    await runner.run()

    runner.run_once.assert_not_called()


# ---------------------------------------------------------------------------
# 3. End-to-End Flow: Redis Event -> Processor -> Scheduler -> DB -> XACK
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_end_to_end_flow_with_database_integration(
    session: AsyncSession,
    clean_database: None,
) -> None:
    job = Job(
        name="e2e-job",
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=50,
        status=JobStatus.QUEUED,
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["python"],
        max_retries=3,
    )
    worker = Worker(
        name="e2e-worker",
        version="0.1.0",
        status=WorkerStatus.READY,
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=0,
        capabilities=["python"],
    )
    session.add_all([job, worker])
    await session.commit()

    job_id = job.id
    worker_id = worker.id

    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock()
    mock_redis.xack = AsyncMock()

    # Redis returns no pending, but one new JOB_CREATED event
    message_id = "1700000000099-0"
    mock_redis.xreadgroup.side_effect = [
        [],  # consume_pending -> empty
        [("events:jobs", [(message_id, {"event_type": "JOB_CREATED", "job_id": str(job_id)})])],  # consume -> new event
    ]

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
    )
    scheduler = Scheduler(session)
    processor = SchedulerEventProcessor(scheduler=scheduler)
    runner = SchedulerRunner(consumer=consumer, processor=processor)

    # Execute one scheduling iteration
    await runner.run_once()

    # 1. Verify Job transitioned to ASSIGNED
    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.ASSIGNED

    # 2. Verify Attempt created
    persisted_attempt = await session.scalar(select(Attempt).where(Attempt.job_id == job_id))
    assert persisted_attempt is not None
    assert persisted_attempt.worker_id == worker_id
    assert persisted_attempt.attempt_number == 1
    assert persisted_attempt.status == AttemptStatus.CREATED

    # 3. Verify ACTIVE Reservation created
    persisted_reservation = await session.scalar(
        select(Reservation).where(Reservation.attempt_id == persisted_attempt.id)
    )
    assert persisted_reservation is not None
    assert persisted_reservation.worker_id == worker_id
    assert persisted_reservation.cpu_reserved == 2
    assert persisted_reservation.memory_reserved_mb == 2048
    assert persisted_reservation.status == ReservationStatus.ACTIVE

    # 4. Verify Assignment created
    persisted_assignment = await session.scalar(
        select(Assignment).where(Assignment.attempt_id == persisted_attempt.id)
    )
    assert persisted_assignment is not None
    assert persisted_assignment.worker_id == worker_id
    assert persisted_assignment.status == AssignmentStatus.CREATED

    # 5. Verify XACK was issued
    mock_redis.xack.assert_awaited_once_with(
        "events:jobs",
        "scheduler-group",
        message_id,
    )

