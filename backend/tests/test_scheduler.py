import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.exc import NoResultFound
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


def _make_job(
    name: str = "job-1",
    status: JobStatus = JobStatus.QUEUED,
    priority: int = 50,
    created_at: datetime | None = None,
    cpu_required: int = 2,
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


def _make_reservation(
    cpu_reserved: int = 0,
    memory_reserved_mb: int = 0,
    gpu_reserved: int = 0,
    status: ReservationStatus = ReservationStatus.ACTIVE,
) -> Reservation:
    return Reservation(
        cpu_reserved=cpu_reserved,
        memory_reserved_mb=memory_reserved_mb,
        gpu_reserved=gpu_reserved,
        status=status,
    )


def _make_worker(
    name: str = "worker-1",
    status: WorkerStatus = WorkerStatus.READY,
    cpu_capacity: int = 8,
    memory_capacity_mb: int = 16384,
    gpu_capacity: int = 1,
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
# 1. Job selection (get_next_job)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_next_job_returns_none_when_no_jobs_exist(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = await scheduler.get_next_job()

    assert job is None


@pytest.mark.asyncio
async def test_get_next_job_only_selects_queued_jobs(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    non_queued_jobs = [
        _make_job(name="job-assigned", status=JobStatus.ASSIGNED, priority=100),
        _make_job(name="job-running", status=JobStatus.RUNNING, priority=100),
        _make_job(name="job-succeeded", status=JobStatus.SUCCEEDED, priority=100),
        _make_job(name="job-failed", status=JobStatus.FAILED, priority=100),
        _make_job(name="job-cancelled", status=JobStatus.CANCELLED, priority=100),
    ]
    session.add_all(non_queued_jobs)
    await session.commit()

    assert await scheduler.get_next_job() is None

    queued_job = _make_job(name="job-queued", status=JobStatus.QUEUED, priority=10)
    session.add(queued_job)
    await session.commit()

    selected = await scheduler.get_next_job()
    assert selected is not None
    assert selected.name == "job-queued"
    assert selected.status == JobStatus.QUEUED


@pytest.mark.asyncio
async def test_get_next_job_returns_single_job(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    jobs = [
        _make_job(name="job-1", priority=10),
        _make_job(name="job-2", priority=20),
        _make_job(name="job-3", priority=30),
    ]
    session.add_all(jobs)
    await session.commit()

    selected = await scheduler.get_next_job()

    assert isinstance(selected, Job)
    assert selected.name == "job-3"


@pytest.mark.asyncio
async def test_get_next_job_selects_higher_priority_even_if_created_later(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)
    now = datetime.now(UTC)

    older_low_priority = _make_job(
        name="older-low-pri",
        priority=10,
        created_at=now - timedelta(hours=2),
    )
    newer_high_priority = _make_job(
        name="newer-high-pri",
        priority=90,
        created_at=now,
    )
    session.add_all([older_low_priority, newer_high_priority])
    await session.commit()

    selected = await scheduler.get_next_job()

    assert selected is not None
    assert selected.name == "newer-high-pri"
    assert selected.priority == 90


@pytest.mark.asyncio
async def test_get_next_job_breaks_priority_tie_by_created_at_asc(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)
    now = datetime.now(UTC)

    older_job = _make_job(
        name="older-job",
        priority=50,
        created_at=now - timedelta(minutes=30),
    )
    newer_job = _make_job(
        name="newer-job",
        priority=50,
        created_at=now - timedelta(minutes=5),
    )
    session.add_all([newer_job, older_job])
    await session.commit()

    selected = await scheduler.get_next_job()

    assert selected is not None
    assert selected.name == "older-job"


@pytest.mark.asyncio
async def test_get_next_job_query_compilation_and_ordering() -> None:
    session = AsyncMock(spec=AsyncSession)
    mock_result = Mock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute.return_value = mock_result

    scheduler = Scheduler(session)
    result = await scheduler.get_next_job()

    assert result is None
    session.execute.assert_awaited_once()
    statement = session.execute.call_args[0][0]

    compiled_sql = str(statement.compile())
    assert "WHERE jobs.status = :status_1" in compiled_sql or "WHERE jobs.status =" in compiled_sql
    assert "ORDER BY jobs.priority DESC, jobs.created_at ASC" in compiled_sql
    assert "LIMIT :param_1" in compiled_sql or "LIMIT 1" in compiled_sql
    assert statement._for_update_arg is None


# ---------------------------------------------------------------------------
# 2. READY Worker discovery (get_ready_workers)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_ready_workers_returns_only_ready_workers(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    workers = [
        _make_worker(name="worker-ready-1", status=WorkerStatus.READY),
        _make_worker(name="worker-ready-2", status=WorkerStatus.READY),
        _make_worker(name="worker-registering", status=WorkerStatus.REGISTERING),
        _make_worker(name="worker-busy", status=WorkerStatus.BUSY),
        _make_worker(name="worker-draining", status=WorkerStatus.DRAINING),
        _make_worker(name="worker-offline", status=WorkerStatus.OFFLINE),
    ]
    session.add_all(workers)
    await session.commit()

    ready_workers = await scheduler.get_ready_workers()

    ready_names = {w.name for w in ready_workers}
    assert ready_names == {"worker-ready-1", "worker-ready-2"}
    assert all(w.status == WorkerStatus.READY for w in ready_workers)


@pytest.mark.asyncio
async def test_get_ready_workers_does_not_modify_worker_state(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    worker = _make_worker(name="worker-ready", status=WorkerStatus.READY)
    session.add(worker)
    await session.commit()

    ready_workers = await scheduler.get_ready_workers()
    assert len(ready_workers) == 1
    assert ready_workers[0].status == WorkerStatus.READY
    assert not session.is_modified(ready_workers[0])


@pytest.mark.asyncio
async def test_get_ready_workers_query_compilation() -> None:
    session = AsyncMock(spec=AsyncSession)
    mock_result = Mock()
    mock_result.scalars.return_value.all.return_value = []
    session.execute.return_value = mock_result

    scheduler = Scheduler(session)
    result = await scheduler.get_ready_workers()

    assert result == []
    session.execute.assert_awaited_once()
    statement = session.execute.call_args[0][0]

    compiled_sql = str(statement.compile())
    assert (
        "WHERE workers.status = :status_1" in compiled_sql
        or "WHERE workers.status =" in compiled_sql
    )


# ---------------------------------------------------------------------------
# 3. Available resources (get_available_resources)
# ---------------------------------------------------------------------------


def test_get_available_resources_no_reservations() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=2,
    )

    available = scheduler.get_available_resources(worker)

    assert available == (8, 16384, 2)


def test_get_available_resources_active_reservations_subtracted() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    res1 = _make_reservation(
        cpu_reserved=2,
        memory_reserved_mb=4096,
        gpu_reserved=1,
        status=ReservationStatus.ACTIVE,
    )
    res2 = _make_reservation(
        cpu_reserved=1,
        memory_reserved_mb=2048,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=2,
        reservations=[res1, res2],
    )

    available = scheduler.get_available_resources(worker)

    # 8 - (2 + 1) = 5, 16384 - (4096 + 2048) = 10240, 2 - (1 + 0) = 1
    assert available == (5, 10240, 1)


def test_get_available_resources_released_reservations_ignored() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    released_res = _make_reservation(
        cpu_reserved=4,
        memory_reserved_mb=8192,
        gpu_reserved=1,
        status=ReservationStatus.RELEASED,
    )
    worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=2,
        reservations=[released_res],
    )

    available = scheduler.get_available_resources(worker)

    assert available == (8, 16384, 2)


def test_get_available_resources_mixed_active_and_released() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    active_res = _make_reservation(
        cpu_reserved=3,
        memory_reserved_mb=4096,
        gpu_reserved=1,
        status=ReservationStatus.ACTIVE,
    )
    released_res = _make_reservation(
        cpu_reserved=4,
        memory_reserved_mb=8192,
        gpu_reserved=1,
        status=ReservationStatus.RELEASED,
    )
    worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=2,
        reservations=[active_res, released_res],
    )

    available = scheduler.get_available_resources(worker)

    # Only active_res is subtracted: 8 - 3 = 5, 16384 - 4096 = 12288, 2 - 1 = 1
    assert available == (5, 12288, 1)


# ---------------------------------------------------------------------------
# 4. Worker filtering (filter_workers)
# ---------------------------------------------------------------------------


def test_filter_workers_resource_capacities() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=4,
        memory_required_mb=4096,
        gpu_required=1,
        required_capabilities=["docker"],
    )

    eligible_worker = _make_worker(
        name="eligible",
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],
    )
    insufficient_cpu = _make_worker(
        name="low-cpu",
        cpu_capacity=3,
        memory_capacity_mb=8192,
        gpu_capacity=2,
        capabilities=["docker"],
    )
    insufficient_memory = _make_worker(
        name="low-mem",
        cpu_capacity=8,
        memory_capacity_mb=2048,
        gpu_capacity=2,
        capabilities=["docker"],
    )
    insufficient_gpu = _make_worker(
        name="low-gpu",
        cpu_capacity=8,
        memory_capacity_mb=8192,
        gpu_capacity=0,
        capabilities=["docker"],
    )

    filtered = scheduler.filter_workers(
        job,
        [eligible_worker, insufficient_cpu, insufficient_memory, insufficient_gpu],
    )

    assert filtered == [eligible_worker]


def test_filter_workers_rejects_insufficient_available_resources_due_to_active_reservations() -> (
    None
):
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=4,
        memory_required_mb=4096,
        gpu_required=1,
        required_capabilities=["docker"],
    )

    # Active reservation leaves insufficient CPU: 4 - 1 = 3 < 4
    res_cpu = _make_reservation(
        cpu_reserved=1,
        memory_reserved_mb=0,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    worker_low_avail_cpu = _make_worker(
        name="low-avail-cpu",
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[res_cpu],
    )

    # Active reservation leaves insufficient memory: 4096 - 2048 = 2048 < 4096
    res_mem = _make_reservation(
        cpu_reserved=0,
        memory_reserved_mb=2048,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    worker_low_avail_mem = _make_worker(
        name="low-avail-mem",
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[res_mem],
    )

    # Active reservation leaves insufficient GPU: 1 - 1 = 0 < 1
    res_gpu = _make_reservation(
        cpu_reserved=0,
        memory_reserved_mb=0,
        gpu_reserved=1,
        status=ReservationStatus.ACTIVE,
    )
    worker_low_avail_gpu = _make_worker(
        name="low-avail-gpu",
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[res_gpu],
    )

    filtered = scheduler.filter_workers(
        job,
        [worker_low_avail_cpu, worker_low_avail_mem, worker_low_avail_gpu],
    )

    assert filtered == []


def test_filter_workers_accepts_when_available_resources_satisfy_job() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["docker"],
    )

    active_res = _make_reservation(
        cpu_reserved=2,
        memory_reserved_mb=2048,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    released_res = _make_reservation(
        cpu_reserved=4,
        memory_reserved_mb=8192,
        gpu_reserved=1,
        status=ReservationStatus.RELEASED,
    )
    worker = _make_worker(
        name="worker-sufficient-available",
        cpu_capacity=8,
        memory_capacity_mb=8192,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[active_res, released_res],
    )

    filtered = scheduler.filter_workers(job, [worker])
    assert filtered == [worker]


def test_filter_workers_capabilities() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))

    # Case A: all required capabilities present -> eligible
    job_with_caps = _make_job(
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=0,
        required_capabilities=["docker", "python"],
    )
    all_caps_worker = _make_worker(
        name="all-caps",
        cpu_capacity=4,
        memory_capacity_mb=2048,
        gpu_capacity=0,
        capabilities=["docker", "python", "cuda"],
    )
    missing_one_cap = _make_worker(
        name="missing-python",
        cpu_capacity=4,
        memory_capacity_mb=2048,
        gpu_capacity=0,
        capabilities=["docker", "cuda"],
    )

    filtered = scheduler.filter_workers(job_with_caps, [all_caps_worker, missing_one_cap])
    assert filtered == [all_caps_worker]

    # Case B: no required capabilities -> eligible if resources fit
    job_no_caps = _make_job(
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=0,
        required_capabilities=[],
    )
    worker_no_caps = _make_worker(
        name="no-caps",
        cpu_capacity=2,
        memory_capacity_mb=1024,
        gpu_capacity=0,
        capabilities=[],
    )
    worker_with_some_caps = _make_worker(
        name="some-caps",
        cpu_capacity=2,
        memory_capacity_mb=1024,
        gpu_capacity=0,
        capabilities=["python"],
    )

    filtered_no_caps = scheduler.filter_workers(
        job_no_caps,
        [worker_no_caps, worker_with_some_caps],
    )
    assert filtered_no_caps == [worker_no_caps, worker_with_some_caps]


def test_filter_workers_still_enforces_capabilities_with_reservations() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["docker", "gpu"],
    )

    worker = _make_worker(
        name="worker-no-gpu-cap",
        cpu_capacity=8,
        memory_capacity_mb=8192,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[
            _make_reservation(
                cpu_reserved=1,
                memory_reserved_mb=1024,
                gpu_reserved=0,
                status=ReservationStatus.ACTIVE,
            )
        ],
    )

    filtered = scheduler.filter_workers(job, [worker])
    assert filtered == []


# ---------------------------------------------------------------------------
# 5. Worker scoring (score_worker)
# ---------------------------------------------------------------------------


def test_score_worker_calculation() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=2,
        memory_required_mb=4096,
        gpu_required=1,
    )
    worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=3,
    )

    score = scheduler.score_worker(job, worker)

    # (8 - 2, 16384 - 4096, 3 - 1) = (6, 12288, 2)
    assert score == (6, 12288, 2)


def test_score_worker_exact_match() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=4,
        memory_required_mb=8192,
        gpu_required=2,
    )
    worker = _make_worker(
        cpu_capacity=4,
        memory_capacity_mb=8192,
        gpu_capacity=2,
    )

    score = scheduler.score_worker(job, worker)

    assert score == (0, 0, 0)


def test_score_worker_calculated_from_available_resources_after_active_reservations() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=1,
    )
    active_res = _make_reservation(
        cpu_reserved=2,
        memory_reserved_mb=4096,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=2,
        reservations=[active_res],
    )

    score = scheduler.score_worker(job, worker)

    # available = (8 - 2, 16384 - 4096, 2 - 0) = (6, 12288, 2)
    # score = (6 - 2, 12288 - 2048, 2 - 1) = (4, 10240, 1)
    assert score == (4, 10240, 1)


def test_score_worker_released_reservations_do_not_affect_score() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=1,
    )
    active_res = _make_reservation(
        cpu_reserved=2,
        memory_reserved_mb=4096,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    released_res = _make_reservation(
        cpu_reserved=4,
        memory_reserved_mb=8192,
        gpu_reserved=1,
        status=ReservationStatus.RELEASED,
    )
    worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=2,
        reservations=[active_res, released_res],
    )

    score = scheduler.score_worker(job, worker)

    # score matches active_res only: (4, 10240, 1)
    assert score == (4, 10240, 1)


# ---------------------------------------------------------------------------
# 6. Worker selection (select_worker)
# ---------------------------------------------------------------------------


def test_select_worker_empty_list_returns_none() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job()

    selected = scheduler.select_worker(job, [])

    assert selected is None


def test_select_worker_picks_smallest_score() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(cpu_required=2, memory_required_mb=2048, gpu_required=0)

    # Worker scores for this job:
    # w_large: (8 - 2, 16384 - 2048, 0 - 0) = (6, 14336, 0)
    # w_med:   (4 - 2, 4096 - 2048, 0 - 0)  = (2, 2048, 0)
    # w_tight: (2 - 2, 2048 - 2048, 0 - 0)  = (0, 0, 0)
    w_large = _make_worker(name="large", cpu_capacity=8, memory_capacity_mb=16384, gpu_capacity=0)
    w_med = _make_worker(name="med", cpu_capacity=4, memory_capacity_mb=4096, gpu_capacity=0)
    w_tight = _make_worker(name="tight", cpu_capacity=2, memory_capacity_mb=2048, gpu_capacity=0)

    selected = scheduler.select_worker(job, [w_large, w_med, w_tight])
    assert selected is w_tight

    selected_reversed = scheduler.select_worker(job, [w_tight, w_med, w_large])
    assert selected_reversed is w_tight


def test_select_worker_tuple_ordering() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(cpu_required=2, memory_required_mb=2048, gpu_required=0)

    # w1: cpu slack 1, mem slack 4096 -> score (1, 4096, 0)
    # w2: cpu slack 2, mem slack 1024 -> score (2, 1024, 0)
    # Tuple comparison compares CPU slack first: (1, 4096, 0) < (2, 1024, 0)
    w1 = _make_worker(name="w1", cpu_capacity=3, memory_capacity_mb=6144, gpu_capacity=0)
    w2 = _make_worker(name="w2", cpu_capacity=4, memory_capacity_mb=3072, gpu_capacity=0)

    selected = scheduler.select_worker(job, [w2, w1])
    assert selected is w1


def test_select_worker_tie_breaking_order() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(cpu_required=2, memory_required_mb=2048, gpu_required=0)

    # Both workers have identical capacities and score (2, 2048, 0)
    w_first = _make_worker(name="first", cpu_capacity=4, memory_capacity_mb=4096, gpu_capacity=0)
    w_second = _make_worker(name="second", cpu_capacity=4, memory_capacity_mb=4096, gpu_capacity=0)

    # min() returns the first item encountered in case of a tie
    assert scheduler.select_worker(job, [w_first, w_second]) is w_first
    assert scheduler.select_worker(job, [w_second, w_first]) is w_second


# ---------------------------------------------------------------------------
# 7. Scheduling transaction (create_attempt, create_reservation, create_assignment, mark_job_assigned, schedule_job)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_attempt_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job_id = uuid4()
    worker_id = uuid4()
    job = _make_job()
    job.id = job_id
    worker = _make_worker()
    worker.id = worker_id

    attempt = await scheduler.create_attempt(job, worker)

    assert attempt.job_id == job_id
    assert attempt.worker_id == worker_id
    assert attempt.attempt_number == 1
    assert attempt.status == AttemptStatus.CREATED
    session.add.assert_called_once_with(attempt)
    session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_attempt_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(name="job-attempt-db")
    worker = _make_worker(name="worker-attempt-db")
    session.add_all([job, worker])
    await session.commit()

    attempt = await scheduler.create_attempt(job, worker)

    assert attempt.id is not None
    assert attempt.job_id == job.id
    assert attempt.worker_id == worker.id
    assert attempt.attempt_number == 1
    assert attempt.status == AttemptStatus.CREATED


@pytest.mark.asyncio
async def test_create_reservation_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    worker_id = uuid4()
    attempt_id = uuid4()
    job = _make_job(cpu_required=4, memory_required_mb=8192, gpu_required=2)
    worker = _make_worker()
    worker.id = worker_id
    attempt = Attempt(
        job_id=job.id,
        worker_id=worker_id,
        attempt_number=1,
        status=AttemptStatus.CREATED,
    )
    attempt.id = attempt_id

    reservation = await scheduler.create_reservation(job, worker, attempt)

    assert reservation.worker_id == worker_id
    assert reservation.attempt_id == attempt_id
    assert reservation.cpu_reserved == 4
    assert reservation.memory_reserved_mb == 8192
    assert reservation.gpu_reserved == 2
    assert reservation.status == ReservationStatus.ACTIVE
    session.add.assert_called_once_with(reservation)
    session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_reservation_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(name="job-res-db", cpu_required=3, memory_required_mb=4096, gpu_required=1)
    worker = _make_worker(name="worker-res-db")
    session.add_all([job, worker])
    await session.commit()

    attempt = await scheduler.create_attempt(job, worker)
    reservation = await scheduler.create_reservation(job, worker, attempt)

    assert reservation.id is not None
    assert reservation.worker_id == worker.id
    assert reservation.attempt_id == attempt.id
    assert reservation.cpu_reserved == 3
    assert reservation.memory_reserved_mb == 4096
    assert reservation.gpu_reserved == 1
    assert reservation.status == ReservationStatus.ACTIVE


@pytest.mark.asyncio
async def test_create_assignment_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    worker_id = uuid4()
    attempt_id = uuid4()
    worker = _make_worker()
    worker.id = worker_id
    attempt = Attempt(
        job_id=uuid4(),
        worker_id=worker_id,
        attempt_number=1,
        status=AttemptStatus.CREATED,
    )
    attempt.id = attempt_id

    assignment = await scheduler.create_assignment(attempt, worker)

    assert assignment.attempt_id == attempt_id
    assert assignment.worker_id == worker_id
    assert assignment.status == AssignmentStatus.CREATED
    session.add.assert_called_once_with(assignment)
    session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_assignment_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(name="job-asgn-db")
    worker = _make_worker(name="worker-asgn-db")
    session.add_all([job, worker])
    await session.commit()

    attempt = await scheduler.create_attempt(job, worker)
    assignment = await scheduler.create_assignment(attempt, worker)

    assert assignment.id is not None
    assert assignment.attempt_id == attempt.id
    assert assignment.worker_id == worker.id
    assert assignment.status == AssignmentStatus.CREATED


def test_mark_job_assigned() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(status=JobStatus.QUEUED)

    scheduler.mark_job_assigned(job)

    assert job.status == JobStatus.ASSIGNED


@pytest.mark.asyncio
async def test_schedule_job_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job_id = uuid4()
    worker_id = uuid4()
    job = _make_job(
        status=JobStatus.QUEUED, cpu_required=2, memory_required_mb=2048, gpu_required=0
    )
    job.id = job_id
    worker = _make_worker()
    worker.id = worker_id

    session.get.return_value = job
    scheduler.lock_worker = AsyncMock(return_value=worker)

    assignment = await scheduler.schedule_job(job, worker)

    assert job.status == JobStatus.ASSIGNED
    assert assignment.worker_id == worker_id
    assert assignment.status == AssignmentStatus.CREATED
    session.get.assert_awaited_once_with(Job, job_id, with_for_update=True, populate_existing=True)
    scheduler.lock_worker.assert_awaited_once_with(worker)
    assert session.add.call_count == 4
    assert session.flush.await_count == 3
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_schedule_job_db_persists_all_records(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(
        name="job-schedule-db",
        status=JobStatus.QUEUED,
        cpu_required=2,
        memory_required_mb=4096,
        gpu_required=1,
    )
    worker = _make_worker(name="worker-schedule-db", status=WorkerStatus.READY)
    session.add_all([job, worker])
    await session.commit()

    job_id = job.id
    worker_id = worker.id

    assignment = await scheduler.schedule_job(job, worker)

    assert assignment.id is not None
    assert assignment.status == AssignmentStatus.CREATED

    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.ASSIGNED

    persisted_attempt = await session.scalar(select(Attempt).where(Attempt.job_id == job_id))
    assert persisted_attempt is not None
    assert persisted_attempt.worker_id == worker_id
    assert persisted_attempt.attempt_number == 1
    assert persisted_attempt.status == AttemptStatus.CREATED

    persisted_reservation = await session.scalar(
        select(Reservation).where(Reservation.attempt_id == persisted_attempt.id)
    )
    assert persisted_reservation is not None
    assert persisted_reservation.worker_id == worker_id
    assert persisted_reservation.cpu_reserved == 2
    assert persisted_reservation.memory_reserved_mb == 4096
    assert persisted_reservation.gpu_reserved == 1
    assert persisted_reservation.status == ReservationStatus.ACTIVE

    persisted_assignment = await session.scalar(
        select(Assignment).where(Assignment.attempt_id == persisted_attempt.id)
    )
    assert persisted_assignment is not None
    assert persisted_assignment.id == assignment.id
    assert persisted_assignment.worker_id == worker_id
    assert persisted_assignment.status == AssignmentStatus.CREATED


@pytest.mark.asyncio
async def test_schedule_job_unit_transaction_failure_does_not_commit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job(status=JobStatus.QUEUED)
    job.id = uuid4()
    worker = _make_worker()
    worker.id = uuid4()

    session.get.return_value = job
    scheduler.lock_worker = AsyncMock(return_value=worker)
    scheduler.create_reservation = AsyncMock(
        side_effect=RuntimeError("Reservation creation failed")
    )

    with pytest.raises(RuntimeError, match="Reservation creation failed"):
        await scheduler.schedule_job(job, worker)

    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_job_db_failure_rolls_back_cleanly(
    session: AsyncSession,
    clean_database: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(name="job-fail-db", status=JobStatus.QUEUED)
    worker = _make_worker(name="worker-fail-db", status=WorkerStatus.READY)
    session.add_all([job, worker])
    await session.commit()

    job_id = job.id
    worker_id = worker.id

    async def _failing_create_assignment(attempt: Attempt, worker: Worker) -> Assignment:
        raise RuntimeError("Assignment failure simulation")

    monkeypatch.setattr(scheduler, "create_assignment", _failing_create_assignment)

    with pytest.raises(RuntimeError, match="Assignment failure simulation"):
        await scheduler.schedule_job(job, worker)

    await session.rollback()

    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.QUEUED

    attempts = (await session.scalars(select(Attempt).where(Attempt.job_id == job_id))).all()
    assert len(attempts) == 0

    reservations = (
        await session.scalars(select(Reservation).where(Reservation.worker_id == worker_id))
    ).all()
    assert len(reservations) == 0

    assignments = (
        await session.scalars(select(Assignment).where(Assignment.worker_id == worker_id))
    ).all()
    assert len(assignments) == 0


# ---------------------------------------------------------------------------
# 8. Concurrency safety (lock_worker, validate_worker_resources, schedule_job)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_worker_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    worker = _make_worker()
    worker.id = uuid4()

    mock_result = Mock()
    mock_result.scalar_one.return_value = worker
    session.execute.return_value = mock_result

    result = await scheduler.lock_worker(worker)

    assert result is worker
    session.execute.assert_awaited_once()
    statement = session.execute.call_args[0][0]
    compiled_sql = str(statement.compile())
    assert "WHERE workers.id = :id_1" in compiled_sql or "WHERE workers.id =" in compiled_sql
    assert statement._for_update_arg is not None


@pytest.mark.asyncio
async def test_lock_worker_not_found_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    worker = _make_worker()
    worker.id = uuid4()

    mock_result = Mock()
    mock_result.scalar_one.side_effect = NoResultFound("Worker not found")
    session.execute.return_value = mock_result

    with pytest.raises(NoResultFound):
        await scheduler.lock_worker(worker)


@pytest.mark.asyncio
async def test_lock_worker_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    worker = _make_worker(name="worker-lock-db", status=WorkerStatus.READY)
    session.add(worker)
    await session.commit()

    locked_worker = await scheduler.lock_worker(worker)
    assert locked_worker.id == worker.id
    assert locked_worker.name == "worker-lock-db"


@pytest.mark.asyncio
async def test_lock_worker_not_found_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    worker = _make_worker(name="worker-nonexistent")
    worker.id = uuid4()

    with pytest.raises(NoResultFound):
        await scheduler.lock_worker(worker)


def test_validate_worker_resources_sufficient() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=4,
        memory_required_mb=4096,
        gpu_required=1,
        required_capabilities=["docker", "python"],
    )
    worker = _make_worker(
        cpu_capacity=8,
        memory_capacity_mb=8192,
        gpu_capacity=2,
        capabilities=["docker", "python", "cuda"],
    )

    assert scheduler.validate_worker_resources(job, worker) is True


def test_validate_worker_resources_insufficient_due_to_active_reservations() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=4,
        memory_required_mb=4096,
        gpu_required=1,
        required_capabilities=["docker"],
    )

    # Active CPU reservation leaves only 4 - 2 = 2 CPUs (< 4 required)
    res_cpu = _make_reservation(
        cpu_reserved=2,
        memory_reserved_mb=0,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    worker_low_cpu = _make_worker(
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[res_cpu],
    )
    assert scheduler.validate_worker_resources(job, worker_low_cpu) is False

    # Active memory reservation leaves only 4096 - 2048 = 2048 MB (< 4096 required)
    res_mem = _make_reservation(
        cpu_reserved=0,
        memory_reserved_mb=2048,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    worker_low_mem = _make_worker(
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[res_mem],
    )
    assert scheduler.validate_worker_resources(job, worker_low_mem) is False

    # Active GPU reservation leaves only 1 - 1 = 0 GPU (< 1 required)
    res_gpu = _make_reservation(
        cpu_reserved=0,
        memory_reserved_mb=0,
        gpu_reserved=1,
        status=ReservationStatus.ACTIVE,
    )
    worker_low_gpu = _make_worker(
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[res_gpu],
    )
    assert scheduler.validate_worker_resources(job, worker_low_gpu) is False


def test_validate_worker_resources_released_reservations_ignored() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=4,
        memory_required_mb=4096,
        gpu_required=1,
        required_capabilities=["docker"],
    )
    res_released = _make_reservation(
        cpu_reserved=4,
        memory_reserved_mb=4096,
        gpu_reserved=1,
        status=ReservationStatus.RELEASED,
    )
    worker = _make_worker(
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],
        reservations=[res_released],
    )
    assert scheduler.validate_worker_resources(job, worker) is True


def test_validate_worker_resources_missing_capabilities() -> None:
    scheduler = Scheduler(AsyncMock(spec=AsyncSession))
    job = _make_job(
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["docker", "gpu"],
    )
    worker = _make_worker(
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=1,
        capabilities=["docker"],  # Missing "gpu"
    )
    assert scheduler.validate_worker_resources(job, worker) is False


@pytest.mark.asyncio
async def test_schedule_job_raises_when_job_not_found_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job()
    job.id = uuid4()
    worker = _make_worker()
    worker.id = uuid4()

    session.get.return_value = None

    with pytest.raises(ValueError, match="Job not found"):
        await scheduler.schedule_job(job, worker)

    session.get.assert_awaited_once_with(Job, job.id, with_for_update=True, populate_existing=True)
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_job_rechecks_job_queued_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job(status=JobStatus.ASSIGNED)
    job.id = uuid4()
    worker = _make_worker()
    worker.id = uuid4()

    session.get.return_value = job

    with pytest.raises(ValueError, match="Job is no longer queued"):
        await scheduler.schedule_job(job, worker)

    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_job_rechecks_job_queued_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(name="job-nonqueued-db", status=JobStatus.ASSIGNED)
    worker = _make_worker(name="worker-nonqueued-db", status=WorkerStatus.READY)
    session.add_all([job, worker])
    await session.commit()

    with pytest.raises(ValueError, match="Job is no longer queued"):
        await scheduler.schedule_job(job, worker)


@pytest.mark.asyncio
async def test_schedule_job_db_job_not_found(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(name="job-not-in-db")
    job.id = uuid4()
    worker = _make_worker(name="worker-db-found", status=WorkerStatus.READY)
    session.add(worker)
    await session.commit()

    with pytest.raises(ValueError, match="Job not found"):
        await scheduler.schedule_job(job, worker)


@pytest.mark.asyncio
async def test_schedule_job_stale_resources_rejected_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job(status=JobStatus.QUEUED, cpu_required=4)
    job.id = uuid4()

    # Initial candidate worker appeared to have 4 CPUs
    initial_worker = _make_worker(cpu_capacity=4)
    initial_worker.id = uuid4()

    # But locked worker has an active reservation taking 2 CPUs
    res = _make_reservation(cpu_reserved=2, status=ReservationStatus.ACTIVE)
    locked_worker = _make_worker(cpu_capacity=4, reservations=[res])
    locked_worker.id = initial_worker.id

    session.get.return_value = job
    scheduler.lock_worker = AsyncMock(return_value=locked_worker)

    with pytest.raises(ValueError, match="Worker no longer has sufficient resources"):
        await scheduler.schedule_job(job, initial_worker)

    session.commit.assert_not_awaited()
    assert session.add.call_count == 0


@pytest.mark.asyncio
async def test_schedule_job_stale_resources_rejected_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(name="job-stale-res-db", status=JobStatus.QUEUED, cpu_required=4)
    worker = _make_worker(name="worker-stale-res-db", status=WorkerStatus.READY, cpu_capacity=4)
    session.add_all([job, worker])
    await session.commit()

    job_id = job.id
    worker_id = worker.id

    # Simulate another attempt taking 2 CPUs on worker
    other_job = _make_job(name="job-other-db", status=JobStatus.RUNNING)
    session.add(other_job)
    await session.flush()

    attempt_other = Attempt(
        job_id=other_job.id,
        worker_id=worker_id,
        attempt_number=1,
        status=AttemptStatus.RUNNING,
    )
    session.add(attempt_other)
    await session.flush()

    res_other = Reservation(
        worker_id=worker_id,
        attempt_id=attempt_other.id,
        cpu_reserved=2,
        memory_reserved_mb=0,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    session.add(res_other)
    await session.commit()

    # Authoritative validation detects worker now only has 4 - 2 = 2 CPUs (< 4 required)
    with pytest.raises(ValueError, match="Worker no longer has sufficient resources"):
        await scheduler.schedule_job(job, worker)

    await session.rollback()

    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.QUEUED

    attempts_for_job = (
        await session.scalars(select(Attempt).where(Attempt.job_id == job_id))
    ).all()
    assert len(attempts_for_job) == 0


# ---------------------------------------------------------------------------
# 9. Complete scheduling flow (schedule_next_job)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_next_job_no_queued_job_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    scheduler.get_next_job = AsyncMock(return_value=None)
    scheduler.get_ready_workers = AsyncMock()
    scheduler.schedule_job = AsyncMock()

    result = await scheduler.schedule_next_job()

    assert result is None
    scheduler.get_next_job.assert_awaited_once()
    scheduler.get_ready_workers.assert_not_called()
    scheduler.schedule_job.assert_not_called()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_next_job_no_queued_job_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    # Database has a ready worker but no queued jobs
    worker = _make_worker(name="worker-idle-db", status=WorkerStatus.READY)
    session.add(worker)
    await session.commit()

    result = await scheduler.schedule_next_job()

    assert result is None
    attempts = (await session.scalars(select(Attempt))).all()
    assert len(attempts) == 0
    reservations = (await session.scalars(select(Reservation))).all()
    assert len(reservations) == 0
    assignments = (await session.scalars(select(Assignment))).all()
    assert len(assignments) == 0


@pytest.mark.asyncio
async def test_schedule_next_job_no_ready_workers_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job(status=JobStatus.QUEUED)
    scheduler.get_next_job = AsyncMock(return_value=job)
    scheduler.get_ready_workers = AsyncMock(return_value=[])
    scheduler.schedule_job = AsyncMock()

    result = await scheduler.schedule_next_job()

    assert result is None
    assert job.status == JobStatus.QUEUED
    scheduler.get_next_job.assert_awaited_once()
    scheduler.get_ready_workers.assert_awaited_once()
    scheduler.schedule_job.assert_not_called()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_next_job_no_ready_workers_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(name="job-queued-no-workers", status=JobStatus.QUEUED)
    worker_busy = _make_worker(name="worker-busy-db", status=WorkerStatus.BUSY)
    worker_offline = _make_worker(name="worker-offline-db", status=WorkerStatus.OFFLINE)
    session.add_all([job, worker_busy, worker_offline])
    await session.commit()

    job_id = job.id

    result = await scheduler.schedule_next_job()

    assert result is None

    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.QUEUED

    attempts = (await session.scalars(select(Attempt))).all()
    assert len(attempts) == 0
    reservations = (await session.scalars(select(Reservation))).all()
    assert len(reservations) == 0
    assignments = (await session.scalars(select(Assignment))).all()
    assert len(assignments) == 0


@pytest.mark.asyncio
async def test_schedule_next_job_no_eligible_workers_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job(status=JobStatus.QUEUED, cpu_required=8, required_capabilities=["cuda"])
    worker1 = _make_worker(name="w1", cpu_capacity=4, capabilities=["cuda"])  # insufficient CPU
    worker2 = _make_worker(name="w2", cpu_capacity=8, capabilities=["docker"])  # missing capability

    scheduler.get_next_job = AsyncMock(return_value=job)
    scheduler.get_ready_workers = AsyncMock(return_value=[worker1, worker2])
    scheduler.schedule_job = AsyncMock()

    result = await scheduler.schedule_next_job()

    assert result is None
    assert job.status == JobStatus.QUEUED
    scheduler.schedule_job.assert_not_called()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_next_job_no_eligible_workers_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(
        name="job-heavy-req",
        status=JobStatus.QUEUED,
        cpu_required=16,
        memory_required_mb=32768,
        required_capabilities=["gpu"],
    )
    worker = _make_worker(
        name="worker-small-ready",
        status=WorkerStatus.READY,
        cpu_capacity=4,
        memory_capacity_mb=4096,
        capabilities=["docker", "python"],
    )
    session.add_all([job, worker])
    await session.commit()

    job_id = job.id

    result = await scheduler.schedule_next_job()

    assert result is None

    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.QUEUED

    attempts = (await session.scalars(select(Attempt))).all()
    assert len(attempts) == 0
    reservations = (await session.scalars(select(Reservation))).all()
    assert len(reservations) == 0
    assignments = (await session.scalars(select(Assignment))).all()
    assert len(assignments) == 0


@pytest.mark.asyncio
async def test_schedule_next_job_single_eligible_worker_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job_id = uuid4()
    worker_id = uuid4()

    job = _make_job(
        status=JobStatus.QUEUED,
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["python"],
    )
    job.id = job_id

    worker = _make_worker(
        status=WorkerStatus.READY,
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=0,
        capabilities=["python"],
    )
    worker.id = worker_id

    scheduler.get_next_job = AsyncMock(return_value=job)
    scheduler.get_ready_workers = AsyncMock(return_value=[worker])
    session.get.return_value = job
    scheduler.lock_worker = AsyncMock(return_value=worker)

    assignment = await scheduler.schedule_next_job()

    assert assignment is not None
    assert assignment.worker_id == worker_id
    assert assignment.status == AssignmentStatus.CREATED
    assert job.status == JobStatus.ASSIGNED
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_schedule_next_job_single_eligible_worker_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(
        name="job-schedule-next-db",
        status=JobStatus.QUEUED,
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["python"],
    )
    worker = _make_worker(
        name="worker-schedule-next-db",
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

    assignment = await scheduler.schedule_next_job()

    assert assignment is not None
    assert assignment.worker_id == worker_id
    assert assignment.status == AssignmentStatus.CREATED

    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.ASSIGNED

    persisted_attempt = await session.scalar(select(Attempt).where(Attempt.job_id == job_id))
    assert persisted_attempt is not None
    assert persisted_attempt.worker_id == worker_id
    assert persisted_attempt.attempt_number == 1
    assert persisted_attempt.status == AttemptStatus.CREATED

    persisted_reservation = await session.scalar(
        select(Reservation).where(Reservation.attempt_id == persisted_attempt.id)
    )
    assert persisted_reservation is not None
    assert persisted_reservation.worker_id == worker_id
    assert persisted_reservation.cpu_reserved == 2
    assert persisted_reservation.memory_reserved_mb == 2048
    assert persisted_reservation.status == ReservationStatus.ACTIVE

    persisted_assignment = await session.scalar(
        select(Assignment).where(Assignment.attempt_id == persisted_attempt.id)
    )
    assert persisted_assignment is not None
    assert persisted_assignment.id == assignment.id
    assert persisted_assignment.worker_id == worker_id


@pytest.mark.asyncio
async def test_schedule_next_job_multiple_eligible_workers_scoring_unit() -> None:
    session = AsyncMock(spec=AsyncSession)
    scheduler = Scheduler(session)

    job = _make_job(
        status=JobStatus.QUEUED,
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["python"],
    )
    job.id = uuid4()

    w_large = _make_worker(
        name="large",
        status=WorkerStatus.READY,
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=0,
        capabilities=["python"],
    )
    w_large.id = uuid4()

    w_tight = _make_worker(
        name="tight",
        status=WorkerStatus.READY,
        cpu_capacity=2,
        memory_capacity_mb=2048,
        gpu_capacity=0,
        capabilities=["python"],
    )
    w_tight.id = uuid4()

    scheduler.get_next_job = AsyncMock(return_value=job)
    scheduler.get_ready_workers = AsyncMock(return_value=[w_large, w_tight])
    expected_assignment = Assignment(
        attempt_id=uuid4(),
        worker_id=w_tight.id,
        status=AssignmentStatus.CREATED,
    )
    scheduler.schedule_job = AsyncMock(return_value=expected_assignment)

    assignment = await scheduler.schedule_next_job()

    assert assignment is expected_assignment
    scheduler.schedule_job.assert_awaited_once_with(job, w_tight)


@pytest.mark.asyncio
async def test_schedule_next_job_multiple_eligible_workers_scoring_db(
    session: AsyncSession,
    clean_database: None,
) -> None:
    scheduler = Scheduler(session)

    job = _make_job(
        name="job-multi-workers-db",
        status=JobStatus.QUEUED,
        cpu_required=2,
        memory_required_mb=2048,
        gpu_required=0,
        required_capabilities=["python"],
    )
    w_large = _make_worker(
        name="worker-large-score-db",
        status=WorkerStatus.READY,
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=0,
        capabilities=["python"],
    )
    w_tight = _make_worker(
        name="worker-tight-score-db",
        status=WorkerStatus.READY,
        cpu_capacity=2,
        memory_capacity_mb=2048,
        gpu_capacity=0,
        capabilities=["python"],
    )
    session.add_all([job, w_large, w_tight])
    await session.commit()

    job_id = job.id
    tight_worker_id = w_tight.id

    assignment = await scheduler.schedule_next_job()

    assert assignment is not None
    assert assignment.worker_id == tight_worker_id

    persisted_job = await session.scalar(select(Job).where(Job.id == job_id))
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.ASSIGNED

    persisted_attempt = await session.scalar(select(Attempt).where(Attempt.job_id == job_id))
    assert persisted_attempt is not None
    assert persisted_attempt.worker_id == tight_worker_id
