import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from forge.db.config import get_settings
from forge.db.models import (
    Assignment,
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
    await session.execute(delete(Reservation))
    await session.execute(delete(Assignment))
    await session.execute(delete(Attempt))
    await session.execute(delete(Job))
    await session.execute(delete(Worker))
    await session.commit()
    yield
    await session.execute(delete(Reservation))
    await session.execute(delete(Assignment))
    await session.execute(delete(Attempt))
    await session.execute(delete(Job))
    await session.execute(delete(Worker))
    await session.commit()


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


async def _create_worker(
    session: AsyncSession,
    *,
    name: str | None = None,
    status: WorkerStatus = WorkerStatus.READY,
    last_heartbeat_at: datetime | None = None,
    cpu_capacity: int = 8,
    memory_capacity_mb: int = 16384,
) -> Worker:
    worker = Worker(
        id=uuid4(),
        name=name or f"worker-{uuid4().hex[:8]}",
        version="0.1.0",
        cpu_capacity=cpu_capacity,
        memory_capacity_mb=memory_capacity_mb,
        gpu_capacity=0,
        capabilities=["docker", "python"],
        status=status,
        last_heartbeat_at=last_heartbeat_at,
    )
    session.add(worker)
    await session.commit()
    return worker


async def _create_job(
    session: AsyncSession,
    *,
    cpu_required: int = 2,
    memory_required_mb: int = 2048,
) -> Job:
    job = Job(
        id=uuid4(),
        name=f"job-{uuid4().hex[:8]}",
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=50,
        cpu_required=cpu_required,
        memory_required_mb=memory_required_mb,
        gpu_required=0,
        required_capabilities=["python"],
        status=JobStatus.QUEUED,
        max_retries=1,
    )
    session.add(job)
    await session.commit()
    return job


async def _create_reservation(
    session: AsyncSession,
    worker: Worker,
    *,
    status: ReservationStatus = ReservationStatus.ACTIVE,
    cpu_reserved: int = 2,
    memory_reserved_mb: int = 2048,
    released_at: datetime | None = None,
) -> tuple[Attempt, Reservation]:
    job = await _create_job(session, cpu_required=cpu_reserved, memory_required_mb=memory_reserved_mb)
    attempt = Attempt(
        id=uuid4(),
        job_id=job.id,
        worker_id=worker.id,
        attempt_number=1,
        status=AttemptStatus.RUNNING,
    )
    session.add(attempt)
    await session.flush()

    reservation = Reservation(
        id=uuid4(),
        worker_id=worker.id,
        attempt_id=attempt.id,
        cpu_reserved=cpu_reserved,
        memory_reserved_mb=memory_reserved_mb,
        gpu_reserved=0,
        status=status,
        released_at=released_at,
    )
    session.add(reservation)
    await session.commit()
    return attempt, reservation


# ---------------------------------------------------------------------------
# 1. Heartbeat Expiry Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fresh_worker_remains_unchanged(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Fresh worker with recent heartbeat is not marked OFFLINE."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.READY,
        last_heartbeat_at=now - timedelta(seconds=5),
    )

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    reaped = await scheduler.reap_expired_workers(now=now)

    assert len(reaped) == 0
    refreshed = await session.get(Worker, worker.id)
    assert refreshed is not None
    assert refreshed.status == WorkerStatus.READY


@pytest.mark.asyncio
async def test_expired_ready_worker_becomes_offline(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Expired READY worker is marked OFFLINE."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.READY,
        last_heartbeat_at=now - timedelta(seconds=35),
    )

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    reaped = await scheduler.reap_expired_workers(now=now)

    assert len(reaped) == 1
    assert reaped[0].id == worker.id
    refreshed = await session.get(Worker, worker.id)
    assert refreshed is not None
    assert refreshed.status == WorkerStatus.OFFLINE


@pytest.mark.asyncio
async def test_expired_busy_worker_becomes_offline(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Expired BUSY worker is marked OFFLINE."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.BUSY,
        last_heartbeat_at=now - timedelta(seconds=40),
    )

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    reaped = await scheduler.reap_expired_workers(now=now)

    assert len(reaped) == 1
    refreshed = await session.get(Worker, worker.id)
    assert refreshed is not None
    assert refreshed.status == WorkerStatus.OFFLINE


@pytest.mark.asyncio
async def test_expired_draining_worker_becomes_offline(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Expired DRAINING worker is marked OFFLINE."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.DRAINING,
        last_heartbeat_at=now - timedelta(seconds=40),
    )

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    reaped = await scheduler.reap_expired_workers(now=now)

    assert len(reaped) == 1
    refreshed = await session.get(Worker, worker.id)
    assert refreshed is not None
    assert refreshed.status == WorkerStatus.OFFLINE


@pytest.mark.asyncio
async def test_already_offline_worker_remains_offline_without_mutation(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Already OFFLINE worker is skipped and not returned as newly reaped."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.OFFLINE,
        last_heartbeat_at=now - timedelta(seconds=100),
    )

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    reaped = await scheduler.reap_expired_workers(now=now)

    assert len(reaped) == 0
    refreshed = await session.get(Worker, worker.id)
    assert refreshed is not None
    assert refreshed.status == WorkerStatus.OFFLINE


@pytest.mark.asyncio
async def test_multiple_workers_selective_reaping(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Only expired workers are reaped; active workers remain untouched."""
    now = datetime.now(UTC)
    fresh_worker = await _create_worker(
        session,
        name="fresh-worker",
        status=WorkerStatus.READY,
        last_heartbeat_at=now - timedelta(seconds=10),
    )
    expired_worker_1 = await _create_worker(
        session,
        name="expired-worker-1",
        status=WorkerStatus.READY,
        last_heartbeat_at=now - timedelta(seconds=35),
    )
    expired_worker_2 = await _create_worker(
        session,
        name="expired-worker-2",
        status=WorkerStatus.BUSY,
        last_heartbeat_at=now - timedelta(seconds=60),
    )

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    reaped = await scheduler.reap_expired_workers(now=now)

    reaped_ids = {w.id for w in reaped}
    assert reaped_ids == {expired_worker_1.id, expired_worker_2.id}

    fresh_refreshed = await session.get(Worker, fresh_worker.id)
    assert fresh_refreshed is not None
    assert fresh_refreshed.status == WorkerStatus.READY

    exp1_refreshed = await session.get(Worker, expired_worker_1.id)
    assert exp1_refreshed is not None
    assert exp1_refreshed.status == WorkerStatus.OFFLINE

    exp2_refreshed = await session.get(Worker, expired_worker_2.id)
    assert exp2_refreshed is not None
    assert exp2_refreshed.status == WorkerStatus.OFFLINE


@pytest.mark.asyncio
async def test_worker_with_null_heartbeat_uses_created_at(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Worker with last_heartbeat_at=None falls back to created_at."""
    now = datetime.now(UTC)
    # Worker created 40s ago without any heartbeats
    worker = await _create_worker(
        session,
        status=WorkerStatus.READY,
        last_heartbeat_at=None,
    )
    # Manually update created_at to 40s ago
    worker.created_at = now - timedelta(seconds=40)
    await session.commit()

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    reaped = await scheduler.reap_expired_workers(now=now)

    assert len(reaped) == 1
    assert reaped[0].id == worker.id
    refreshed = await session.get(Worker, worker.id)
    assert refreshed is not None
    assert refreshed.status == WorkerStatus.OFFLINE


# ---------------------------------------------------------------------------
# 2. Reservation Release on Heartbeat Expiry Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_worker_releases_active_reservations(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """When a worker expires, its ACTIVE reservations are RELEASED with released_at set."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.BUSY,
        last_heartbeat_at=now - timedelta(seconds=50),
    )
    _, reservation = await _create_reservation(session, worker, status=ReservationStatus.ACTIVE)

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    await scheduler.reap_expired_workers(now=now)

    res = await session.get(Reservation, reservation.id)
    assert res is not None
    assert res.status == ReservationStatus.RELEASED
    assert res.released_at == now


@pytest.mark.asyncio
async def test_expired_worker_releases_multiple_active_reservations(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """All ACTIVE reservations on an expired worker are released."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.BUSY,
        last_heartbeat_at=now - timedelta(seconds=50),
    )
    _, res1 = await _create_reservation(session, worker, status=ReservationStatus.ACTIVE)
    _, res2 = await _create_reservation(session, worker, status=ReservationStatus.ACTIVE)

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    await scheduler.reap_expired_workers(now=now)

    refreshed_res1 = await session.get(Reservation, res1.id)
    refreshed_res2 = await session.get(Reservation, res2.id)

    assert refreshed_res1 is not None and refreshed_res1.status == ReservationStatus.RELEASED
    assert refreshed_res2 is not None and refreshed_res2.status == ReservationStatus.RELEASED
    assert refreshed_res1.released_at == now
    assert refreshed_res2.released_at == now


@pytest.mark.asyncio
async def test_already_released_reservations_remain_untouched(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Already RELEASED reservations are not modified and their released_at is preserved."""
    now = datetime.now(UTC)
    original_released_at = now - timedelta(minutes=10)
    worker = await _create_worker(
        session,
        status=WorkerStatus.BUSY,
        last_heartbeat_at=now - timedelta(seconds=50),
    )
    _, res_released = await _create_reservation(
        session,
        worker,
        status=ReservationStatus.RELEASED,
        released_at=original_released_at,
    )
    _, res_active = await _create_reservation(
        session,
        worker,
        status=ReservationStatus.ACTIVE,
    )

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    await scheduler.reap_expired_workers(now=now)

    refreshed_released = await session.get(Reservation, res_released.id)
    assert refreshed_released is not None
    assert refreshed_released.status == ReservationStatus.RELEASED
    assert refreshed_released.released_at == original_released_at

    refreshed_active = await session.get(Reservation, res_active.id)
    assert refreshed_active is not None
    assert refreshed_active.status == ReservationStatus.RELEASED
    assert refreshed_active.released_at == now


@pytest.mark.asyncio
async def test_resource_availability_reflects_released_reservations(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """After reaping, get_available_resources no longer subtracts the released reservations."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.BUSY,
        last_heartbeat_at=now - timedelta(seconds=50),
        cpu_capacity=8,
        memory_capacity_mb=16384,
    )
    await _create_reservation(
        session,
        worker,
        cpu_reserved=6,
        memory_reserved_mb=8192,
        status=ReservationStatus.ACTIVE,
    )

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)

    # Before reaping: 8 - 6 = 2 CPU available
    result = await session.execute(
        select(Worker).options(selectinload(Worker.reservations)).where(Worker.id == worker.id)
    )
    w_before = result.scalar_one()
    avail_cpu_before, avail_mem_before, _ = scheduler.get_available_resources(w_before)
    assert avail_cpu_before == 2
    assert avail_mem_before == 8192

    # Reap expired worker
    await scheduler.reap_expired_workers(now=now)

    # After reaping: all 8 CPU and 16384 MB available
    result = await session.execute(
        select(Worker).options(selectinload(Worker.reservations)).where(Worker.id == worker.id)
    )
    w_after = result.scalar_one()
    avail_cpu_after, avail_mem_after, _ = scheduler.get_available_resources(w_after)
    assert avail_cpu_after == 8
    assert avail_mem_after == 16384


# ---------------------------------------------------------------------------
# 3. Scheduling Eligibility Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_offline_worker_is_ineligible_for_scheduling(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Expired worker marked OFFLINE is not returned by get_ready_workers and cannot be scheduled."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.READY,
        last_heartbeat_at=now - timedelta(seconds=50),
    )
    job = await _create_job(session)

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)

    # Reap expired worker
    await scheduler.reap_expired_workers(now=now)

    # Ineligible for ready workers
    ready_workers = await scheduler.get_ready_workers()
    assert worker.id not in {w.id for w in ready_workers}

    # schedule_next_job returns None because no ready workers exist
    assignment = await scheduler.schedule_next_job()
    assert assignment is None


# ---------------------------------------------------------------------------
# 4. Idempotency Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaper_idempotency_on_repeated_execution(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """Running reap_expired_workers twice is safe and causes no duplicate modifications."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.READY,
        last_heartbeat_at=now - timedelta(seconds=50),
    )
    _, reservation = await _create_reservation(session, worker, status=ReservationStatus.ACTIVE)

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)

    # First run
    reaped_1 = await scheduler.reap_expired_workers(now=now)
    assert len(reaped_1) == 1

    res_1 = await session.get(Reservation, reservation.id)
    assert res_1 is not None
    assert res_1.status == ReservationStatus.RELEASED
    assert res_1.released_at == now

    # Second run at a slightly later time
    later = now + timedelta(seconds=10)
    reaped_2 = await scheduler.reap_expired_workers(now=later)
    assert len(reaped_2) == 0

    res_2 = await session.get(Reservation, reservation.id)
    assert res_2 is not None
    assert res_2.status == ReservationStatus.RELEASED
    # released_at must NOT be updated to 'later'
    assert res_2.released_at == now


# ---------------------------------------------------------------------------
# 5. Transaction Rollback Behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaper_transaction_rollback_on_failure(
    session: AsyncSession,
    clean_database: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the reaper transaction fails, both worker status and reservations remain unchanged."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.READY,
        last_heartbeat_at=now - timedelta(seconds=50),
    )
    _, reservation = await _create_reservation(session, worker, status=ReservationStatus.ACTIVE)

    worker_id = worker.id
    res_id = reservation.id

    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)

    # Force commit to raise
    async def _failing_commit() -> None:
        raise RuntimeError("Simulated database failure during reaper commit")

    monkeypatch.setattr(session, "commit", _failing_commit)

    with pytest.raises(RuntimeError, match="Simulated database failure"):
        await scheduler.reap_expired_workers(now=now)

    # Rollback should have preserved initial states in a fresh session query
    engine = create_async_engine(_database_url())
    async with async_sessionmaker(engine, expire_on_commit=False)() as fresh_session:
        persisted_worker = await fresh_session.get(Worker, worker_id)
        persisted_res = await fresh_session.get(Reservation, res_id)

    await engine.dispose()

    assert persisted_worker is not None
    assert persisted_worker.status == WorkerStatus.READY
    assert persisted_res is not None
    assert persisted_res.status == ReservationStatus.ACTIVE


# ---------------------------------------------------------------------------
# 6. Lifecycle Integration (Runner invokes reaper)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_runner_run_once_invokes_reaper(
    session: AsyncSession,
    clean_database: None,
) -> None:
    """SchedulerRunner.run_once() automatically invokes reap_expired_workers."""
    now = datetime.now(UTC)
    worker = await _create_worker(
        session,
        status=WorkerStatus.READY,
        last_heartbeat_at=now - timedelta(seconds=60),
    )

    mock_redis = AsyncMock()
    mock_redis.xreadgroup = AsyncMock(return_value=[])
    mock_redis.xautoclaim = AsyncMock(return_value=("0-0", [], []))

    consumer = SchedulerEventConsumer(
        redis=mock_redis,
        stream_name="events:jobs",
        consumer_group="scheduler-group",
        consumer_name="scheduler-worker-1",
        pending_min_idle_ms=60_000,
    )
    scheduler = Scheduler(session, heartbeat_timeout_seconds=30)
    processor = SchedulerEventProcessor(scheduler=scheduler)
    runner = SchedulerRunner(consumer=consumer, processor=processor)

    # Execute run_once
    await runner.run_once()

    # Verify worker was reaped to OFFLINE
    refreshed = await session.get(Worker, worker.id)
    assert refreshed is not None
    assert refreshed.status == WorkerStatus.OFFLINE
