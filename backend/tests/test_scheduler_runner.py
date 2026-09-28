import asyncio
import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from redis import asyncio as aioredis
from redis.asyncio import Redis
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from forge.db.config import get_settings
from forge.db.models import (
    Assignment,
    AssignmentStatus,
    Attempt,
    AttemptStatus,
    Job,
    JobEvent,
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
    consumer.consume_stale_pending = AsyncMock(return_value=[])
    consumer.consume = AsyncMock(return_value=[])
    consumer.acknowledge = AsyncMock()
    return consumer


@pytest.fixture
def mock_processor() -> AsyncMock:
    processor = AsyncMock(spec=SchedulerEventProcessor)
    processor.process = AsyncMock()
    processor.scheduler = AsyncMock(spec=Scheduler)
    processor.scheduler.reap_expired_workers = AsyncMock(return_value=[])
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
# 1. SchedulerRunner.run_once() Unit Tests (Section C & D)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_once_processes_pending_messages_before_new(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    """Current pending has priority over stale pending and new messages."""
    pending_msg = ("1700000000001-0", {"event_type": "JOB_CREATED", "job_id": "job-1"})
    mock_consumer.consume_pending.return_value = [pending_msg]

    await runner.run_once()

    # Pending message is processed and acknowledged
    mock_processor.process.assert_awaited_once_with("1700000000001-0", pending_msg[1])
    mock_consumer.acknowledge.assert_awaited_once_with("1700000000001-0")

    # When pending messages exist, neither stale pending nor new messages are consumed
    mock_consumer.consume_stale_pending.assert_not_called()
    mock_consumer.consume.assert_not_called()


@pytest.mark.asyncio
async def test_run_once_prioritizes_stale_pending_over_new_when_no_current_pending(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    """Stale pending has priority over new messages when no current pending messages exist."""
    mock_consumer.consume_pending.return_value = []
    stale_msg = ("1700000000002-0", {"event_type": "JOB_CREATED", "job_id": "job-stale"})
    mock_consumer.consume_stale_pending.return_value = [stale_msg]

    await runner.run_once()

    mock_consumer.consume_pending.assert_awaited_once()
    mock_consumer.consume_stale_pending.assert_awaited_once()
    mock_processor.process.assert_awaited_once_with("1700000000002-0", stale_msg[1])
    mock_consumer.acknowledge.assert_awaited_once_with("1700000000002-0")

    # When stale pending messages exist, new messages are NOT consumed
    mock_consumer.consume.assert_not_called()


@pytest.mark.asyncio
async def test_run_once_processes_new_messages_when_no_pending_and_no_stale(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    """New messages are consumed only when both current pending and stale pending are empty."""
    mock_consumer.consume_pending.return_value = []
    mock_consumer.consume_stale_pending.return_value = []
    new_msg = ("1700000000003-0", {"event_type": "JOB_CREATED", "job_id": "job-new"})
    mock_consumer.consume.return_value = [new_msg]

    await runner.run_once()

    mock_consumer.consume_pending.assert_awaited_once()
    mock_consumer.consume_stale_pending.assert_awaited_once()
    mock_consumer.consume.assert_awaited_once()
    mock_processor.process.assert_awaited_once_with("1700000000003-0", new_msg[1])
    mock_consumer.acknowledge.assert_awaited_once_with("1700000000003-0")


@pytest.mark.asyncio
async def test_run_once_does_not_acknowledge_when_processing_fails(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    mock_consumer.consume_pending.return_value = []
    mock_consumer.consume_stale_pending.return_value = []
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
    mock_consumer.consume_stale_pending.assert_not_called()
    mock_consumer.consume.assert_not_called()


@pytest.mark.asyncio
async def test_run_once_does_not_acknowledge_when_stale_pending_processing_fails(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    """Section D: When stale pending processing fails, message is not acknowledged and error propagates."""
    mock_consumer.consume_pending.return_value = []
    stale_msg = ("1700000000005-0", {"event_type": "JOB_CREATED", "job_id": "job-stale-fail"})
    mock_consumer.consume_stale_pending.return_value = [stale_msg]

    mock_processor.process.side_effect = RuntimeError("Stale pending processing error")

    with pytest.raises(RuntimeError, match="Stale pending processing error"):
        await runner.run_once()

    mock_processor.process.assert_awaited_once_with("1700000000005-0", stale_msg[1])
    mock_consumer.acknowledge.assert_not_called()
    mock_consumer.consume.assert_not_called()


@pytest.mark.asyncio
async def test_run_once_processes_multiple_messages_and_acknowledges_each(
    runner: SchedulerRunner,
    mock_consumer: AsyncMock,
    mock_processor: AsyncMock,
) -> None:
    mock_consumer.consume_pending.return_value = []
    mock_consumer.consume_stale_pending.return_value = []
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
    mock_redis.xautoclaim = AsyncMock(return_value=("0-0", [], []))
    mock_redis.xack = AsyncMock()

    # Redis returns no pending, no stale, but one new JOB_CREATED event
    message_id = "1700000000099-0"
    mock_redis.xreadgroup.side_effect = [
        [],  # consume_pending -> empty
        [
            ("events:jobs", [(message_id, {"event_type": "JOB_CREATED", "job_id": str(job_id)})])
        ],  # consume -> new event
    ]

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
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


# ---------------------------------------------------------------------------
# 4. Phase 9 R3 — Event Idempotency DB Integration Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replayed_event_does_not_create_duplicate_records(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """
    Phase 9 R3 tests 2–6:
    Replaying a JOB_CREATED event after the Job is ASSIGNED:
      2. Returns successfully (no exception).
      3. Creates no additional Attempt.
      4. Creates no additional Reservation.
      5. Creates no additional Assignment.
      6. Creates no additional JobEvent.
    """
    job = Job(
        name="idempotency-job",
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
        name="idempotency-worker",
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

    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock()
    mock_redis.xautoclaim = AsyncMock(return_value=("0-0", [], []))
    mock_redis.xack = AsyncMock()

    message_id = "1700000000100-0"

    # First delivery: no pending, no stale, one new event → schedules the job.
    mock_redis.xreadgroup.side_effect = [
        [],  # consume_pending -> empty
        [("events:jobs", [(message_id, {"event_type": "JOB_CREATED", "job_id": str(job_id)})])],
    ]

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
    )
    scheduler = Scheduler(session)
    processor = SchedulerEventProcessor(scheduler=scheduler)
    runner = SchedulerRunner(consumer=consumer, processor=processor)

    # First run: schedules the job (Job → ASSIGNED).
    await runner.run_once()

    # Verify scheduling happened.
    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.ASSIGNED

    attempts_after_first = list(
        (await session.execute(select(Attempt).where(Attempt.job_id == job_id))).scalars()
    )
    reservations_after_first = list(
        (
            await session.execute(
                select(Reservation).where(
                    Reservation.attempt_id == attempts_after_first[0].id
                )
            )
        ).scalars()
    )
    assignments_after_first = list(
        (
            await session.execute(
                select(Assignment).where(
                    Assignment.attempt_id == attempts_after_first[0].id
                )
            )
        ).scalars()
    )
    job_events_after_first = list(
        (await session.execute(select(JobEvent).where(JobEvent.job_id == job_id))).scalars()
    )
    assert len(attempts_after_first) == 1
    assert len(reservations_after_first) == 1
    assert len(assignments_after_first) == 1
    assert len(job_events_after_first) == 1

    # -----------------------------------------------------------------------
    # Replay: same message_id, same job_id — job is now ASSIGNED.
    # -----------------------------------------------------------------------
    mock_redis.xreadgroup.side_effect = [
        [],  # consume_pending -> empty
        [("events:jobs", [(message_id, {"event_type": "JOB_CREATED", "job_id": str(job_id)})])],
    ]
    mock_redis.xack.reset_mock()

    # Second run: replayed event — must return without raising.
    await runner.run_once()

    # Test 2: runner completed without exception (implicit — reached here).

    # Test 3: no additional Attempt.
    attempts_after_replay = list(
        (await session.execute(select(Attempt).where(Attempt.job_id == job_id))).scalars()
    )
    assert len(attempts_after_replay) == 1

    # Test 4: no additional Reservation.
    reservations_after_replay = list(
        (
            await session.execute(
                select(Reservation).where(
                    Reservation.attempt_id == attempts_after_first[0].id
                )
            )
        ).scalars()
    )
    assert len(reservations_after_replay) == 1

    # Test 5: no additional Assignment.
    assignments_after_replay = list(
        (
            await session.execute(
                select(Assignment).where(
                    Assignment.attempt_id == attempts_after_first[0].id
                )
            )
        ).scalars()
    )
    assert len(assignments_after_replay) == 1

    # Test 6: no additional JobEvent.
    job_events_after_replay = list(
        (await session.execute(select(JobEvent).where(JobEvent.job_id == job_id))).scalars()
    )
    assert len(job_events_after_replay) == 1

    # Test 9: runner issued XACK for the replayed event.
    mock_redis.xack.assert_awaited_once_with(
        "events:jobs",
        "scheduler-group",
        message_id,
    )


@pytest.mark.asyncio
async def test_replayed_event_for_missing_job_raises_and_is_not_acknowledged(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """
    Phase 9 R3 test 7:
    A JOB_CREATED event whose job_id does not exist in the DB raises ValueError.
    The runner must NOT ACK the message.
    """
    missing_job_id = uuid4()
    message_id = "1700000000200-0"

    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock()
    mock_redis.xautoclaim = AsyncMock(return_value=("0-0", [], []))
    mock_redis.xack = AsyncMock()
    mock_redis.xreadgroup.side_effect = [
        [],
        [
            (
                "events:jobs",
                [(message_id, {"event_type": "JOB_CREATED", "job_id": str(missing_job_id)})],
            )
        ],
    ]

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
    )
    scheduler = Scheduler(session)
    processor = SchedulerEventProcessor(scheduler=scheduler)
    runner = SchedulerRunner(consumer=consumer, processor=processor)

    with pytest.raises(ValueError, match="not found"):
        await runner.run_once()

    mock_redis.xack.assert_not_called()


@pytest.mark.asyncio
async def test_queued_job_still_follows_existing_scheduling_path(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """
    Phase 9 R3 test 8:
    A QUEUED job still goes through the full scheduling pipeline.
    Confirms the existing path is preserved after the idempotency change.
    """
    job = Job(
        name="queued-path-job",
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=60,
        status=JobStatus.QUEUED,
        cpu_required=1,
        memory_required_mb=512,
        gpu_required=0,
        required_capabilities=["python"],
        max_retries=1,
    )
    worker = Worker(
        name="queued-path-worker",
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
    message_id = "1700000000300-0"

    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock()
    mock_redis.xautoclaim = AsyncMock(return_value=("0-0", [], []))
    mock_redis.xack = AsyncMock()
    mock_redis.xreadgroup.side_effect = [
        [],
        [("events:jobs", [(message_id, {"event_type": "JOB_CREATED", "job_id": str(job_id)})])],
    ]

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
    )
    scheduler = Scheduler(session)
    processor = SchedulerEventProcessor(scheduler=scheduler)
    runner = SchedulerRunner(consumer=consumer, processor=processor)

    await runner.run_once()

    refreshed_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert refreshed_job is not None
    assert refreshed_job.status == JobStatus.ASSIGNED

    attempt = await session.scalar(select(Attempt).where(Attempt.job_id == job_id))
    assert attempt is not None
    assert attempt.status == AttemptStatus.CREATED

    mock_redis.xack.assert_awaited_once_with("events:jobs", "scheduler-group", message_id)


# ---------------------------------------------------------------------------
# 5. Phase 9 R4 — Cross-Consumer Recovery End-to-End Integration (Section E)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def real_redis() -> AsyncGenerator[Redis, None]:
    settings = get_settings()
    client: Redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("Redis is not reachable")

    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_cross_consumer_stale_message_recovery_integration(
    session: AsyncSession,
    clean_database: None,
    real_redis: Redis,
) -> None:
    """
    Section E: Realistic two-consumer cross-recovery scenario:
      1. Job and Worker exist in Postgres.
      2. Event published to Redis stream.
      3. Consumer A reads the message (making it pending under Consumer A) and crashes.
      4. Message idles past pending_min_idle_ms threshold.
      5. Consumer B starts up with short pending_min_idle_ms.
      6. Runner B executes run_once():
         - consume_pending() is empty for B
         - consume_stale_pending() claims the message via XAUTOCLAIM
         - processor schedules the Job (Job -> ASSIGNED)
         - acknowledges message via XACK
      7. Final state:
         - Job is ASSIGNED in Postgres
         - Message is no longer pending in Redis
         - Consumer A no longer owns the message
    """
    stream_name = f"forge:test:events:{uuid4().hex}"
    group_name = f"forge:test:schedulers:{uuid4().hex}"

    await real_redis.xgroup_create(
        name=stream_name,
        groupname=group_name,
        id="0",
        mkstream=True,
    )

    try:
        # Setup Job and Worker in DB
        job = Job(
            name="recovery-job",
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
            name="recovery-worker",
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

        # Publish JOB_CREATED event to Redis stream
        message_id = await real_redis.xadd(
            stream_name,
            {"event_type": "JOB_CREATED", "job_id": str(job_id)},
        )

        # Consumer A consumes the message and crashes (does not ACK)
        consumer_a = SchedulerEventConsumer(
            redis=real_redis,
            stream_name=stream_name,
            consumer_group=group_name,
            consumer_name="consumer-A",
            pending_min_idle_ms=50,
        )
        messages_a = await consumer_a.consume(count=1)
        assert len(messages_a) == 1
        assert messages_a[0][0] == message_id

        # Verify message is in Consumer A's PEL
        pending_before = await real_redis.xpending_range(
            name=stream_name,
            groupname=group_name,
            min="-",
            max="+",
            count=10,
        )
        assert len(pending_before) == 1
        assert pending_before[0]["consumer"] == "consumer-A"

        # Wait past min_idle threshold (50ms)
        await asyncio.sleep(0.06)

        # Consumer B starts up
        consumer_b = SchedulerEventConsumer(
            redis=real_redis,
            stream_name=stream_name,
            consumer_group=group_name,
            consumer_name="consumer-B",
            pending_min_idle_ms=50,
        )
        scheduler = Scheduler(session)
        processor = SchedulerEventProcessor(scheduler=scheduler)
        runner_b = SchedulerRunner(consumer=consumer_b, processor=processor)

        # Runner B runs one iteration
        await runner_b.run_once()

        # 1. Verify Job is now ASSIGNED in database
        persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
        assert persisted_job is not None
        assert persisted_job.status == JobStatus.ASSIGNED

        # 2. Verify Attempt, Reservation, Assignment created
        persisted_attempt = await session.scalar(select(Attempt).where(Attempt.job_id == job_id))
        assert persisted_attempt is not None
        assert persisted_attempt.worker_id == worker_id
        assert persisted_attempt.status == AttemptStatus.CREATED

        persisted_reservation = await session.scalar(
            select(Reservation).where(Reservation.attempt_id == persisted_attempt.id)
        )
        assert persisted_reservation is not None
        assert persisted_reservation.status == ReservationStatus.ACTIVE

        persisted_assignment = await session.scalar(
            select(Assignment).where(Assignment.attempt_id == persisted_attempt.id)
        )
        assert persisted_assignment is not None
        assert persisted_assignment.status == AssignmentStatus.CREATED

        # 3. Verify message was acknowledged in Redis and is no longer pending
        pending_after = await real_redis.xpending_range(
            name=stream_name,
            groupname=group_name,
            min="-",
            max="+",
            count=10,
        )
        assert len(pending_after) == 0

    finally:
        await real_redis.delete(stream_name)
