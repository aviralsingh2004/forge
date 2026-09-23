import asyncio
import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import selectinload

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
async def db_engine(migrated_database: str) -> AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine(migrated_database, pool_size=10, max_overflow=20)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(
    db_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db_engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def clean_database(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[None, None]:
    async with session_factory() as session:
        await session.execute(delete(JobEvent))
        await session.execute(delete(Assignment))
        await session.execute(delete(Reservation))
        await session.execute(delete(Attempt))
        await session.execute(delete(Job))
        await session.execute(delete(Worker))
        await session.commit()
    yield
    async with session_factory() as session:
        await session.execute(delete(JobEvent))
        await session.execute(delete(Assignment))
        await session.execute(delete(Reservation))
        await session.execute(delete(Attempt))
        await session.execute(delete(Job))
        await session.execute(delete(Worker))
        await session.commit()


def _make_job(
    name: str = "job-1",
    status: JobStatus = JobStatus.QUEUED,
    priority: int = 50,
    created_at: datetime | None = None,
    cpu_required: int = 6,
    memory_required_mb: int = 2048,
    gpu_required: int = 0,
    required_capabilities: list[str] | None = None,
) -> Job:
    job = Job(
        name=name,
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=priority,
        status=status,
        cpu_required=cpu_required,
        memory_required_mb=memory_required_mb,
        gpu_required=gpu_required,
        required_capabilities=(
            required_capabilities if required_capabilities is not None else ["python"]
        ),
        max_retries=3,
    )
    if created_at is not None:
        job.created_at = created_at
    return job


def _make_worker(
    name: str = "worker-1",
    status: WorkerStatus = WorkerStatus.READY,
    cpu_capacity: int = 8,
    memory_capacity_mb: int = 16384,
    gpu_capacity: int = 0,
    capabilities: list[str] | None = None,
    reservations: list[Reservation] | None = None,
) -> Worker:
    worker = Worker(
        name=name,
        version="0.1.0",
        status=status,
        cpu_capacity=cpu_capacity,
        memory_capacity_mb=memory_capacity_mb,
        gpu_capacity=gpu_capacity,
        capabilities=capabilities if capabilities is not None else ["docker", "python"],
    )
    if reservations is not None:
        worker.reservations = reservations
    return worker


# ---------------------------------------------------------------------------
# Phase 7: Concurrency & Transactional Resource Allocation Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_scheduling_two_schedulers_single_worker_cpu_limit_db(
    session_factory: async_sessionmaker[AsyncSession],
    clean_database: None,
) -> None:
    """Forge Phase 7 Concurrency Gate:

    - One Worker with 8 CPU capacity.
    - Two QUEUED Jobs (Job A and Job B), each requiring 6 CPU.
    - Run two scheduler instances/concurrent scheduling transactions against the same Worker.
    - Both scheduling attempts execute concurrently to exercise PostgreSQL row locking.

    Assertions:
    1. Exactly one Job becomes ASSIGNED.
    2. Exactly one Job remains QUEUED.
    3. Exactly one Attempt is created.
    4. Exactly one active Reservation exists for the Worker.
    5. The active reservation is 6 CPU, never 12 CPU.
    6. Exactly one Assignment is created.
    7. The assigned Job has a JOB_ASSIGNED JobEvent associated with its Attempt.
    8. The queued Job does not get an assignment, attempt, or active reservation from losing transaction.
    9. No over-allocation occurs even when both schedulers initially observe the Worker as having 8 CPU available.
    """
    # 1. Setup initial state in PostgreSQL
    worker = _make_worker(
        name="worker-concurrency-8cpu",
        status=WorkerStatus.READY,
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=0,
        capabilities=["docker", "python"],
    )
    job_a = _make_job(
        name="job-a-6cpu",
        status=JobStatus.QUEUED,
        priority=50,
        cpu_required=6,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["python"],
    )
    job_b = _make_job(
        name="job-b-6cpu",
        status=JobStatus.QUEUED,
        priority=50,
        cpu_required=6,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["python"],
    )

    async with session_factory() as init_session:
        init_session.add_all([worker, job_a, job_b])
        await init_session.commit()
        worker_id = worker.id
        job_a_id = job_a.id
        job_b_id = job_b.id

    # 2. Open two separate concurrent DB sessions & schedulers
    async with (
        session_factory() as session1,
        session_factory() as session2,
    ):
        scheduler1 = Scheduler(session1)
        scheduler2 = Scheduler(session2)

        # Load worker entity in both sessions
        w1 = await session1.scalar(
            select(Worker).options(selectinload(Worker.reservations)).where(Worker.id == worker_id)
        )
        w2 = await session2.scalar(
            select(Worker).options(selectinload(Worker.reservations)).where(Worker.id == worker_id)
        )
        assert w1 is not None and w2 is not None

        # Precondition check (Assertion 9): Both schedulers initially observe 8 CPU available
        assert scheduler1.get_available_resources(w1) == (8, 16384, 0)
        assert scheduler2.get_available_resources(w2) == (8, 16384, 0)

        ja = await session1.get(Job, job_a_id)
        jb = await session2.get(Job, job_b_id)
        assert ja is not None and jb is not None

        barrier = asyncio.Barrier(2)

        async def run_sched(
            sched: Scheduler,
            target_job: Job,
            target_worker: Worker,
            sess: AsyncSession,
        ) -> tuple[str, object]:
            # Wait for both tasks to be ready to execute concurrently
            await barrier.wait()
            try:
                assignment = await sched.schedule_job(target_job, target_worker)
                return ("success", assignment)
            except Exception as exc:
                await sess.rollback()
                return ("error", exc)

        results = await asyncio.gather(
            run_sched(scheduler1, ja, w1, session1),
            run_sched(scheduler2, jb, w2, session2),
        )

    # Verify that exactly one transaction succeeded and the other failed due to insufficient resources
    statuses = [r[0] for r in results]
    assert statuses.count("success") == 1
    assert statuses.count("error") == 1

    error_result = next(r[1] for r in results if r[0] == "error")
    assert isinstance(error_result, ValueError)
    assert "Worker no longer has sufficient resources" in str(error_result)

    # 3. Assertions on committed PostgreSQL state using a fresh verification session
    async with session_factory() as verif_session:
        persisted_jobs = (
            await verif_session.scalars(select(Job).where(Job.id.in_([job_a_id, job_b_id])))
        ).all()
        assert len(persisted_jobs) == 2

        assigned_jobs = [j for j in persisted_jobs if j.status == JobStatus.ASSIGNED]
        queued_jobs = [j for j in persisted_jobs if j.status == JobStatus.QUEUED]

        # 1. Exactly one Job becomes ASSIGNED
        assert len(assigned_jobs) == 1
        assigned_job = assigned_jobs[0]

        # 2. Exactly one Job remains QUEUED
        assert len(queued_jobs) == 1
        queued_job = queued_jobs[0]

        # 3. Exactly one Attempt is created
        all_attempts = (await verif_session.scalars(select(Attempt))).all()
        assert len(all_attempts) == 1
        attempt = all_attempts[0]
        assert attempt.job_id == assigned_job.id
        assert attempt.worker_id == worker_id
        assert attempt.status == AttemptStatus.CREATED
        assert attempt.attempt_number == 1

        # 4. Exactly one active Reservation exists for the Worker
        all_reservations = (
            await verif_session.scalars(
                select(Reservation).where(Reservation.worker_id == worker_id)
            )
        ).all()
        active_reservations = [r for r in all_reservations if r.status == ReservationStatus.ACTIVE]
        assert len(active_reservations) == 1
        active_res = active_reservations[0]
        assert active_res.attempt_id == attempt.id
        assert active_res.worker_id == worker_id

        # 5. The active reservation is 6 CPU, never 12 CPU
        assert active_res.cpu_reserved == 6
        assert sum(r.cpu_reserved for r in active_reservations) == 6
        assert sum(r.cpu_reserved for r in all_reservations) == 6

        # 6. Exactly one Assignment is created
        all_assignments = (await verif_session.scalars(select(Assignment))).all()
        assert len(all_assignments) == 1
        assignment = all_assignments[0]
        assert assignment.attempt_id == attempt.id
        assert assignment.worker_id == worker_id
        assert assignment.status == AssignmentStatus.CREATED

        # 7. The assigned Job has a JOB_ASSIGNED JobEvent associated with its Attempt
        assigned_job_events = (
            await verif_session.scalars(select(JobEvent).where(JobEvent.job_id == assigned_job.id))
        ).all()
        assert len(assigned_job_events) == 1
        assert assigned_job_events[0].event_type == "JOB_ASSIGNED"
        assert assigned_job_events[0].attempt_id == attempt.id

        # 8. The queued Job does not get an assignment, attempt, or active reservation from losing transaction
        queued_job_attempts = (
            await verif_session.scalars(select(Attempt).where(Attempt.job_id == queued_job.id))
        ).all()
        assert len(queued_job_attempts) == 0

        queued_job_events = (
            await verif_session.scalars(select(JobEvent).where(JobEvent.job_id == queued_job.id))
        ).all()
        assert len(queued_job_events) == 0

        # 9. No over-allocation occurs even when both schedulers initially observe Worker having 8 CPU
        loaded_worker = await verif_session.scalar(
            select(Worker).options(selectinload(Worker.reservations)).where(Worker.id == worker_id)
        )
        assert loaded_worker is not None
        available_resources = Scheduler(verif_session).get_available_resources(loaded_worker)
        assert available_resources == (2, 14336, 0)  # 8 - 6 = 2 CPU, 16384 - 2048 = 14336 MB


@pytest.mark.asyncio
async def test_concurrent_schedule_next_job_two_jobs_single_worker_db(
    session_factory: async_sessionmaker[AsyncSession],
    clean_database: None,
) -> None:
    """Test concurrent schedule_next_job() calls with 2 queued jobs and 1 worker."""
    worker = _make_worker(
        name="worker-next-job-8cpu",
        status=WorkerStatus.READY,
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=0,
        capabilities=["python"],
    )
    job_1 = _make_job(
        name="job-next-1",
        status=JobStatus.QUEUED,
        priority=80,
        created_at=datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC),
        cpu_required=6,
        memory_required_mb=2048,
    )
    job_2 = _make_job(
        name="job-next-2",
        status=JobStatus.QUEUED,
        priority=50,
        created_at=datetime(2026, 1, 1, 11, 0, 0, tzinfo=UTC),
        cpu_required=6,
        memory_required_mb=2048,
    )

    async with session_factory() as init_session:
        init_session.add_all([worker, job_1, job_2])
        await init_session.commit()
        worker_id = worker.id
        job_1_id = job_1.id
        job_2_id = job_2.id

    async with (
        session_factory() as session1,
        session_factory() as session2,
    ):
        scheduler1 = Scheduler(session1)
        scheduler2 = Scheduler(session2)

        barrier = asyncio.Barrier(2)

        async def run_sched_next(sched: Scheduler, sess: AsyncSession) -> tuple[str, object]:
            await barrier.wait()
            try:
                assignment = await sched.schedule_next_job()
                return ("success", assignment)
            except Exception as exc:
                await sess.rollback()
                return ("error", exc)

        results = await asyncio.gather(
            run_sched_next(scheduler1, session1),
            run_sched_next(scheduler2, session2),
        )

    # In schedule_next_job:
    # Task A acquires higher-priority Job 1 and successfully schedules it.
    # Task B either unblocks and sees Worker has insufficient resources, or handles accordingly.
    success_assignments = [r[1] for r in results if r[0] == "success" and r[1] is not None]
    assert len(success_assignments) == 1

    async with session_factory() as verif_session:
        pj1 = await verif_session.scalar(select(Job).where(Job.id == job_1_id))
        pj2 = await verif_session.scalar(select(Job).where(Job.id == job_2_id))
        assert pj1 is not None and pj2 is not None

        assert pj1.status == JobStatus.ASSIGNED
        assert pj2.status == JobStatus.QUEUED

        active_reservations = (
            await verif_session.scalars(
                select(Reservation).where(
                    Reservation.worker_id == worker_id,
                    Reservation.status == ReservationStatus.ACTIVE,
                )
            )
        ).all()
        assert len(active_reservations) == 1
        assert active_reservations[0].cpu_reserved == 6


# ---------------------------------------------------------------------------
# Focused Stale-Resource Revalidation Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_resource_revalidation_insufficient_cpu_db(
    session_factory: async_sessionmaker[AsyncSession],
    clean_database: None,
) -> None:
    """Worker becomes resource-constrained (CPU) after candidate selection but before allocation.

    Verify schedule_job() rejects allocation and does not create an over-capacity reservation.
    """
    worker = _make_worker(
        name="worker-stale-cpu",
        status=WorkerStatus.READY,
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=0,
    )
    job = _make_job(
        name="job-stale-cpu-req",
        status=JobStatus.QUEUED,
        cpu_required=6,
        memory_required_mb=2048,
    )

    async with session_factory() as init_session:
        init_session.add_all([worker, job])
        await init_session.commit()
        worker_id = worker.id
        job_id = job.id

    async with session_factory() as scheduler_session:
        scheduler = Scheduler(scheduler_session)

        # Scheduler selects candidate worker when 8 CPU is available
        candidate_worker = await scheduler_session.scalar(
            select(Worker).options(selectinload(Worker.reservations)).where(Worker.id == worker_id)
        )
        assert candidate_worker is not None
        assert scheduler.get_available_resources(candidate_worker) == (8, 16384, 0)
        assert scheduler.validate_worker_resources(job, candidate_worker) is True

        # Meanwhile, another transaction commits an active 4 CPU reservation
        async with session_factory() as other_session:
            other_job = _make_job(name="other-job", status=JobStatus.RUNNING)
            other_session.add(other_job)
            await other_session.flush()

            other_attempt = Attempt(
                job_id=other_job.id,
                worker_id=worker_id,
                attempt_number=1,
                status=AttemptStatus.RUNNING,
            )
            other_session.add(other_attempt)
            await other_session.flush()

            other_res = Reservation(
                worker_id=worker_id,
                attempt_id=other_attempt.id,
                cpu_reserved=4,
                memory_reserved_mb=1024,
                gpu_reserved=0,
                status=ReservationStatus.ACTIVE,
            )
            other_session.add(other_res)
            await other_session.commit()

        # Now available CPU is 8 - 4 = 4 CPU. Job requires 6 CPU.
        target_job = await scheduler_session.get(Job, job_id)
        assert target_job is not None

        with pytest.raises(ValueError, match="Worker no longer has sufficient resources"):
            await scheduler.schedule_job(target_job, candidate_worker)

        await scheduler_session.rollback()

    # Verify PostgreSQL state: job remains QUEUED, no new reservation/attempt for job
    async with session_factory() as verif_session:
        persisted_job = await verif_session.scalar(select(Job).where(Job.id == job_id))
        assert persisted_job is not None
        assert persisted_job.status == JobStatus.QUEUED

        job_attempts = (
            await verif_session.scalars(select(Attempt).where(Attempt.job_id == job_id))
        ).all()
        assert len(job_attempts) == 0

        worker_reservations = (
            await verif_session.scalars(
                select(Reservation).where(
                    Reservation.worker_id == worker_id,
                    Reservation.status == ReservationStatus.ACTIVE,
                )
            )
        ).all()
        # Only the other job's 4 CPU reservation exists; never 4 + 6 = 10 CPU
        assert len(worker_reservations) == 1
        assert worker_reservations[0].cpu_reserved == 4


@pytest.mark.asyncio
async def test_stale_resource_revalidation_insufficient_memory_db(
    session_factory: async_sessionmaker[AsyncSession],
    clean_database: None,
) -> None:
    """Worker becomes memory-constrained after candidate selection but before allocation."""
    worker = _make_worker(
        name="worker-stale-mem",
        status=WorkerStatus.READY,
        cpu_capacity=8,
        memory_capacity_mb=8192,
        gpu_capacity=0,
    )
    job = _make_job(
        name="job-stale-mem-req",
        status=JobStatus.QUEUED,
        cpu_required=2,
        memory_required_mb=6144,
    )

    async with session_factory() as init_session:
        init_session.add_all([worker, job])
        await init_session.commit()
        worker_id = worker.id
        job_id = job.id

    async with session_factory() as scheduler_session:
        scheduler = Scheduler(scheduler_session)
        candidate_worker = await scheduler_session.scalar(
            select(Worker).options(selectinload(Worker.reservations)).where(Worker.id == worker_id)
        )
        assert candidate_worker is not None

        # Another transaction consumes 4096 MB memory (leaving 8192 - 4096 = 4096 MB < 6144 MB)
        async with session_factory() as other_session:
            other_job = _make_job(name="other-mem-job", status=JobStatus.RUNNING)
            other_session.add(other_job)
            await other_session.flush()

            other_attempt = Attempt(
                job_id=other_job.id,
                worker_id=worker_id,
                attempt_number=1,
                status=AttemptStatus.RUNNING,
            )
            other_session.add(other_attempt)
            await other_session.flush()

            other_res = Reservation(
                worker_id=worker_id,
                attempt_id=other_attempt.id,
                cpu_reserved=1,
                memory_reserved_mb=4096,
                gpu_reserved=0,
                status=ReservationStatus.ACTIVE,
            )
            other_session.add(other_res)
            await other_session.commit()

        target_job = await scheduler_session.get(Job, job_id)
        assert target_job is not None

        with pytest.raises(ValueError, match="Worker no longer has sufficient resources"):
            await scheduler.schedule_job(target_job, candidate_worker)

        await scheduler_session.rollback()

    async with session_factory() as verif_session:
        persisted_job = await verif_session.scalar(select(Job).where(Job.id == job_id))
        assert persisted_job is not None
        assert persisted_job.status == JobStatus.QUEUED

        job_attempts = (
            await verif_session.scalars(select(Attempt).where(Attempt.job_id == job_id))
        ).all()
        assert len(job_attempts) == 0


@pytest.mark.asyncio
async def test_stale_resource_revalidation_unit() -> None:
    """Unit test for stale-resource revalidation during schedule_job()."""
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job_id = uuid4()
    worker_id = uuid4()

    job = _make_job(cpu_required=6, memory_required_mb=2048, gpu_required=0)
    job.id = job_id

    # Candidate worker looked free (8 CPU)
    candidate_worker = _make_worker(cpu_capacity=8, memory_capacity_mb=16384)
    candidate_worker.id = worker_id

    # Locked worker in database has an active reservation of 4 CPU (only 4 CPU left < 6 CPU required)
    stale_active_res = Reservation(
        cpu_reserved=4,
        memory_reserved_mb=1024,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    locked_worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        reservations=[stale_active_res],
    )
    locked_worker.id = worker_id

    session.get.return_value = job
    scheduler.lock_worker = AsyncMock(return_value=locked_worker)

    with pytest.raises(ValueError, match="Worker no longer has sufficient resources"):
        await scheduler.schedule_job(job, candidate_worker)

    # Verify no commits and no new objects added
    session.commit.assert_not_awaited()
    assert session.add.call_count == 0


@pytest.mark.asyncio
async def test_stale_resource_revalidation_missing_capability_unit() -> None:
    """Unit test for missing capability revalidation during schedule_job()."""
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job(
        cpu_required=2,
        memory_required_mb=1024,
        required_capabilities=["gpu", "python"],
    )
    job.id = uuid4()

    candidate_worker = _make_worker(
        cpu_capacity=8,
        capabilities=["gpu", "python"],
    )
    candidate_worker.id = uuid4()

    # Locked worker lost the 'gpu' capability
    locked_worker = _make_worker(
        cpu_capacity=8,
        capabilities=["python"],
        reservations=[],
    )
    locked_worker.id = candidate_worker.id

    session.get.return_value = job
    scheduler.lock_worker = AsyncMock(return_value=locked_worker)

    with pytest.raises(ValueError, match="Worker no longer has sufficient resources"):
        await scheduler.schedule_job(job, candidate_worker)

    session.commit.assert_not_awaited()
    assert session.add.call_count == 0


@pytest.mark.asyncio
async def test_stale_resource_revalidation_insufficient_gpu_unit() -> None:
    """Unit test for insufficient GPU revalidation during schedule_job()."""
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job(
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=1,
    )
    job.id = uuid4()

    candidate_worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=1,
    )
    candidate_worker.id = uuid4()

    # Worker has an active reservation using the only GPU
    stale_res = Reservation(
        cpu_reserved=0,
        memory_reserved_mb=0,
        gpu_reserved=1,
        status=ReservationStatus.ACTIVE,
    )
    locked_worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=1,
        reservations=[stale_res],
    )
    locked_worker.id = candidate_worker.id

    session.get.return_value = job
    scheduler.lock_worker = AsyncMock(return_value=locked_worker)

    with pytest.raises(ValueError, match="Worker no longer has sufficient resources"):
        await scheduler.schedule_job(job, candidate_worker)

    session.commit.assert_not_awaited()
    assert session.add.call_count == 0


@pytest.mark.asyncio
async def test_concurrent_scheduling_simulation_unit() -> None:
    """Simulate two schedulers competing for an 8 CPU worker with two 6 CPU jobs in unit mode."""
    worker_id = uuid4()
    job_a = _make_job(name="job-a", cpu_required=6)
    job_a.id = uuid4()
    job_b = _make_job(name="job-b", cpu_required=6)
    job_b.id = uuid4()

    # Initial state: worker with 8 CPU capacity and 0 reservations
    candidate_worker = _make_worker(cpu_capacity=8, memory_capacity_mb=16384, gpu_capacity=0)
    candidate_worker.id = worker_id

    # 1. Scheduler A succeeds
    session_a = AsyncMock(spec=AsyncSession)
    scheduler_a = Scheduler(session_a)
    session_a.get.return_value = job_a
    # Lock returns worker with 0 active reservations
    locked_worker_a = _make_worker(cpu_capacity=8, memory_capacity_mb=16384, reservations=[])
    locked_worker_a.id = worker_id
    scheduler_a.lock_worker = AsyncMock(return_value=locked_worker_a)

    assignment_a = await scheduler_a.schedule_job(job_a, candidate_worker)
    assert assignment_a is not None
    assert job_a.status == JobStatus.ASSIGNED
    session_a.commit.assert_awaited_once()

    # 2. Scheduler B executes after Scheduler A committed 6 CPU reservation
    session_b = AsyncMock(spec=AsyncSession)
    scheduler_b = Scheduler(session_b)
    session_b.get.return_value = job_b

    # Locked worker now contains the 6 CPU active reservation from Scheduler A
    res_a = Reservation(
        cpu_reserved=6,
        memory_reserved_mb=2048,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    locked_worker_b = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        reservations=[res_a],
    )
    locked_worker_b.id = worker_id
    scheduler_b.lock_worker = AsyncMock(return_value=locked_worker_b)

    with pytest.raises(ValueError, match="Worker no longer has sufficient resources"):
        await scheduler_b.schedule_job(job_b, candidate_worker)

    session_b.commit.assert_not_awaited()
