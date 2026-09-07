import subprocess
import sys
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from forge.db.config import get_settings
from forge.db.models import (
    Assignment,
    AssignmentStatus,
    Attempt,
    Job,
    Reservation,
    ReservationStatus,
    Worker,
    WorkerStatus,
)


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


@pytest.mark.asyncio
async def test_phase_one_persistence_and_constraints(session: AsyncSession) -> None:
    worker = Worker(
        name="worker-integration",
        version="0.1.0",
        status=WorkerStatus.READY,
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=1,
        capabilities=["docker", "python"],
    )
    job = Job(
        name="integration-job",
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=80,
        cpu_required=2,
        memory_required_mb=512,
        gpu_required=0,
        required_capabilities=["python"],
        max_retries=3,
    )
    session.add_all([worker, job])
    await session.flush()

    attempt = Attempt(job=job, attempt_number=1, worker=worker)
    assignment = Assignment(attempt=attempt, worker=worker, status=AssignmentStatus.CREATED)
    reservation = Reservation(
        attempt=attempt,
        worker=worker,
        cpu_reserved=2,
        memory_reserved_mb=512,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    session.add_all([attempt, assignment, reservation])
    await session.commit()

    reservation.status = ReservationStatus.RELEASED
    await session.commit()

    second_attempt = Attempt(job=job, attempt_number=2, worker=worker)
    second_reservation = Reservation(
        attempt=second_attempt,
        worker=worker,
        cpu_reserved=1,
        memory_reserved_mb=256,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    session.add_all([second_attempt, second_reservation])
    await session.commit()

    third_attempt = Attempt(job=job, attempt_number=3, worker=worker)
    third_reservation = Reservation(
        attempt=third_attempt,
        worker=worker,
        cpu_reserved=1,
        memory_reserved_mb=256,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    session.add_all([third_attempt, third_reservation])
    await session.commit()

    active_reservations = await session.scalars(
        select(Reservation).where(
            Reservation.worker_id == worker.id,
            Reservation.status == ReservationStatus.ACTIVE,
        )
    )
    assert len(list(active_reservations)) == 2
    assert second_reservation.attempt_id == second_attempt.id

    loaded_job = await session.scalar(
        select(Job).options(selectinload(Job.attempts)).where(Job.id == job.id)
    )
    loaded_worker = await session.scalar(
        select(Worker).options(selectinload(Worker.reservations)).where(Worker.id == worker.id)
    )
    assert loaded_job is not None
    assert loaded_worker is not None
    assert loaded_job.attempts[0].job_id == job.id
    assert {item.attempt_id for item in loaded_worker.reservations} == {
        attempt.id,
        second_attempt.id,
        third_attempt.id,
    }

    duplicate = Reservation(
        attempt=second_attempt,
        worker=worker,
        cpu_reserved=1,
        memory_reserved_mb=1,
        gpu_reserved=0,
        status=ReservationStatus.ACTIVE,
    )
    session.add(duplicate)
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()

    duplicate_attempt = Attempt(job=job, attempt_number=2, worker=worker)
    session.add(duplicate_attempt)
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()

    duplicate_assignment = Assignment(attempt=attempt, worker=worker)
    session.add(duplicate_assignment)
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()

    negative = Worker(
        name="invalid-worker",
        version="0.1.0",
        status=WorkerStatus.READY,
        cpu_capacity=-1,
        memory_capacity_mb=1,
        gpu_capacity=0,
        capabilities=[],
    )
    session.add(negative)
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()
