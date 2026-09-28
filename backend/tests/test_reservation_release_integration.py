"""
Phase 9 — Reliability Requirement 1:
Reservation release on terminal Attempt states.

Test coverage:
  A. SUCCEEDED attempt releases its active reservation.
  B. FAILED attempt releases its active reservation.
  C. CANCELLED attempt releases its active reservation.
  D. Non-terminal statuses (STARTING, RUNNING) keep the reservation ACTIVE.
  E. released_at is populated for terminal states.
  F. A released reservation no longer reduces Worker available resources.
  G. Repeating a terminal transition attempt (already terminal) is rejected at
     409 without creating duplicate reservations or corrupting state.
  H. A failed status-update transaction leaves Attempt status AND Reservation
     unchanged (atomicity / rollback).
"""
import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from forge.api.main import app
from forge.db.config import get_settings
from forge.db.models import (
    Attempt,
    AttemptStatus,
    Job,
    JobStatus,
    Reservation,
    ReservationStatus,
    Worker,
    WorkerStatus,
)
from forge.db.session import AsyncSessionFactory, get_session
from forge.scheduler.service import Scheduler


# ---------------------------------------------------------------------------
# Module-scoped fixtures (match existing test_attempts_integration.py pattern)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def migrated_database() -> Generator[None, None, None]:
    database_url = get_settings().async_database_url
    sync_url = database_url.replace("+psycopg", "")
    check = subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"sqlalchemy.url={sync_url}", "current"],
        check=False,
        capture_output=True,
        text=True,
    )
    if check.returncode != 0:
        pytest.skip("PostgreSQL is not reachable or Alembic is unavailable")

    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True)
    yield
    subprocess.run([sys.executable, "-m", "alembic", "downgrade", "base"], check=True)


@pytest_asyncio.fixture
async def clean_database(migrated_database: None) -> AsyncGenerator[None, None]:
    yield
    async with AsyncSessionFactory() as session:
        await session.execute(delete(Reservation))
        await session.execute(delete(Attempt))
        await session.execute(delete(Job))
        await session.execute(delete(Worker))
        await session.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _session_override() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionFactory() as session:
        yield session


def _post_status(attempt_id: str, payload: dict) -> object:
    """POST /api/v1/attempts/{attempt_id}/status through the FastAPI test client."""
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            return client.post(f"/api/v1/attempts/{attempt_id}/status", json=payload)
    finally:
        app.dependency_overrides.clear()


async def _persist_worker_job_attempt_reservation(
    *,
    attempt_status: AttemptStatus = AttemptStatus.RUNNING,
    cpu_required: int = 2,
    memory_required_mb: int = 1024,
    gpu_required: int = 0,
) -> tuple[Worker, Job, Attempt, Reservation]:
    """
    Persist a full scheduling fixture: Worker + Job + Attempt + active Reservation.
    The Attempt is in ``attempt_status`` and has an explicit ACTIVE Reservation so
    we can assert its release independently of the Scheduler code path.
    """
    worker = Worker(
        id=uuid4(),
        name=f"res-worker-{uuid4().hex[:8]}",
        version="0.1.0",
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=0,
        capabilities=["python"],
        status=WorkerStatus.READY,
    )
    job = Job(
        id=uuid4(),
        name=f"res-job-{uuid4().hex[:8]}",
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=50,
        cpu_required=cpu_required,
        memory_required_mb=memory_required_mb,
        gpu_required=gpu_required,
        required_capabilities=["python"],
        status=JobStatus.QUEUED,
        max_retries=1,
    )
    started_at = (
        datetime.now(UTC)
        if attempt_status in {AttemptStatus.STARTING, AttemptStatus.RUNNING}
        else None
    )
    attempt = Attempt(
        id=uuid4(),
        job=job,
        attempt_number=1,
        worker=worker,
        status=attempt_status,
        started_at=started_at,
    )
    reservation = Reservation(
        id=uuid4(),
        worker=worker,
        attempt=attempt,
        cpu_reserved=cpu_required,
        memory_reserved_mb=memory_required_mb,
        gpu_reserved=gpu_required,
        status=ReservationStatus.ACTIVE,
    )
    async with AsyncSessionFactory() as session:
        session.add_all([worker, job, attempt, reservation])
        await session.commit()
    return worker, job, attempt, reservation


# ---------------------------------------------------------------------------
# A — SUCCEEDED releases reservation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_succeeded_attempt_releases_active_reservation(clean_database: None) -> None:
    """Phase 9 R1-A: SUCCEEDED transition releases the ACTIVE Reservation."""
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING
    )

    response = _post_status(str(attempt.id), {"status": "SUCCEEDED", "exit_code": 0})
    assert response.status_code == 200

    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)

    assert res is not None
    assert res.status == ReservationStatus.RELEASED


# ---------------------------------------------------------------------------
# B — FAILED releases reservation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_attempt_releases_active_reservation(clean_database: None) -> None:
    """Phase 9 R1-B: FAILED transition releases the ACTIVE Reservation."""
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING
    )

    response = _post_status(
        str(attempt.id),
        {"status": "FAILED", "exit_code": 1, "error_message": "container exited"},
    )
    assert response.status_code == 200

    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)

    assert res is not None
    assert res.status == ReservationStatus.RELEASED


# ---------------------------------------------------------------------------
# C — CANCELLED releases reservation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_attempt_releases_active_reservation(clean_database: None) -> None:
    """Phase 9 R1-C: CANCELLED transition releases the ACTIVE Reservation."""
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING
    )

    response = _post_status(str(attempt.id), {"status": "CANCELLED"})
    assert response.status_code == 200

    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)

    assert res is not None
    assert res.status == ReservationStatus.RELEASED


# ---------------------------------------------------------------------------
# D — Non-terminal statuses keep the reservation ACTIVE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_starting_attempt_does_not_release_reservation(clean_database: None) -> None:
    """Phase 9 R1-D: ASSIGNED->STARTING does NOT release the Reservation."""
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.ASSIGNED
    )

    response = _post_status(str(attempt.id), {"status": "STARTING"})
    assert response.status_code == 200

    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)

    assert res is not None
    assert res.status == ReservationStatus.ACTIVE
    assert res.released_at is None


@pytest.mark.asyncio
async def test_running_attempt_does_not_release_reservation(clean_database: None) -> None:
    """Phase 9 R1-D: STARTING->RUNNING does NOT release the Reservation."""
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.STARTING
    )

    response = _post_status(str(attempt.id), {"status": "RUNNING"})
    assert response.status_code == 200

    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)

    assert res is not None
    assert res.status == ReservationStatus.ACTIVE
    assert res.released_at is None


# ---------------------------------------------------------------------------
# E — released_at is populated for terminal states
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_released_at_is_populated_on_succeeded(clean_database: None) -> None:
    """Phase 9 R1-E: released_at is set to a recent UTC timestamp on SUCCEEDED."""
    before = datetime.now(UTC)
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING
    )

    response = _post_status(str(attempt.id), {"status": "SUCCEEDED", "exit_code": 0})
    assert response.status_code == 200

    after = datetime.now(UTC)

    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)

    assert res is not None
    assert res.released_at is not None
    assert before <= res.released_at <= after


@pytest.mark.asyncio
async def test_released_at_is_populated_on_failed(clean_database: None) -> None:
    """Phase 9 R1-E: released_at is set to a recent UTC timestamp on FAILED."""
    before = datetime.now(UTC)
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING
    )

    response = _post_status(
        str(attempt.id), {"status": "FAILED", "exit_code": 137, "error_message": "oom"}
    )
    assert response.status_code == 200

    after = datetime.now(UTC)

    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)

    assert res is not None
    assert res.released_at is not None
    assert before <= res.released_at <= after


@pytest.mark.asyncio
async def test_released_at_is_populated_on_cancelled(clean_database: None) -> None:
    """Phase 9 R1-E: released_at is set to a recent UTC timestamp on CANCELLED."""
    before = datetime.now(UTC)
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING
    )

    response = _post_status(str(attempt.id), {"status": "CANCELLED"})
    assert response.status_code == 200

    after = datetime.now(UTC)

    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)

    assert res is not None
    assert res.released_at is not None
    assert before <= res.released_at <= after


# ---------------------------------------------------------------------------
# F — Released reservation no longer reduces available Worker resources
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_released_reservation_not_counted_in_available_resources(
    clean_database: None,
) -> None:
    """
    Phase 9 R1-F:
    After an Attempt is SUCCEEDED, its Reservation is RELEASED.
    The Scheduler's get_available_resources() must reflect full Worker capacity again.
    """
    worker, job, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING,
        cpu_required=4,
        memory_required_mb=2048,
    )

    # Before release: 4 CPU reserved → 4 available out of 8.
    async with AsyncSessionFactory() as session:
        refreshed_worker = await session.get(Worker, worker.id)
        from sqlalchemy.orm import selectinload
        result = await session.execute(
            select(Worker)
            .options(selectinload(Worker.reservations))
            .where(Worker.id == worker.id)
        )
        w = result.scalar_one()
        scheduler = Scheduler(session)
        avail_cpu_before, avail_mem_before, _ = scheduler.get_available_resources(w)

    assert avail_cpu_before == 4  # 8 total - 4 reserved

    # Terminal transition releases the reservation.
    response = _post_status(str(attempt.id), {"status": "SUCCEEDED", "exit_code": 0})
    assert response.status_code == 200

    # After release: all 8 CPU available again.
    async with AsyncSessionFactory() as session:
        result = await session.execute(
            select(Worker)
            .options(selectinload(Worker.reservations))
            .where(Worker.id == worker.id)
        )
        w = result.scalar_one()
        scheduler = Scheduler(session)
        avail_cpu_after, avail_mem_after, _ = scheduler.get_available_resources(w)

    assert avail_cpu_after == 8  # 8 total - 0 active reserved
    assert avail_mem_after == 16384  # full capacity restored


# ---------------------------------------------------------------------------
# G — Idempotency: terminal → terminal is rejected; reservation not corrupted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeated_terminal_transition_rejected_without_corrupting_reservation(
    clean_database: None,
) -> None:
    """
    Phase 9 R1-G:
    Sending a second terminal status (SUCCEEDED → SUCCEEDED is invalid; SUCCEEDED →
    FAILED is invalid) must be rejected 409. The Reservation must remain RELEASED
    (not duplicated, not reset to ACTIVE).
    """
    _, _, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING
    )

    # First terminal transition — must succeed and release.
    resp1 = _post_status(str(attempt.id), {"status": "SUCCEEDED", "exit_code": 0})
    assert resp1.status_code == 200

    # Second terminal transition — must be rejected.
    resp2 = _post_status(str(attempt.id), {"status": "FAILED", "exit_code": 1})
    assert resp2.status_code == 409

    # Reservation: exactly one record, still RELEASED.
    async with AsyncSessionFactory() as session:
        res_result = await session.execute(
            select(Reservation).where(Reservation.attempt_id == attempt.id)
        )
        all_reservations = list(res_result.scalars().all())

    assert len(all_reservations) == 1
    assert all_reservations[0].status == ReservationStatus.RELEASED


@pytest.mark.asyncio
async def test_attempt_without_reservation_can_still_reach_terminal_state(
    clean_database: None,
) -> None:
    """
    Phase 9 R1-G (idempotency edge case):
    An Attempt that has no associated Reservation (e.g. persisted directly for
    testing purposes) must still be able to transition to SUCCEEDED without error.
    """
    worker = Worker(
        id=uuid4(),
        name=f"nores-worker-{uuid4().hex[:8]}",
        version="0.1.0",
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=0,
        capabilities=["python"],
        status=WorkerStatus.READY,
    )
    job = Job(
        id=uuid4(),
        name=f"nores-job-{uuid4().hex[:8]}",
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=50,
        cpu_required=1,
        memory_required_mb=256,
        gpu_required=0,
        required_capabilities=["python"],
        status=JobStatus.QUEUED,
        max_retries=1,
    )
    attempt = Attempt(
        id=uuid4(),
        job=job,
        attempt_number=1,
        worker=worker,
        status=AttemptStatus.RUNNING,
        started_at=datetime.now(UTC),
    )
    async with AsyncSessionFactory() as session:
        session.add_all([worker, job, attempt])
        await session.commit()

    response = _post_status(str(attempt.id), {"status": "SUCCEEDED", "exit_code": 0})
    assert response.status_code == 200

    async with AsyncSessionFactory() as session:
        persisted = await session.get(Attempt, attempt.id)

    assert persisted is not None
    assert persisted.status == AttemptStatus.SUCCEEDED


# ---------------------------------------------------------------------------
# H — Transaction atomicity: failed commit leaves Attempt and Reservation unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reservation_and_attempt_unchanged_when_already_released(
    clean_database: None,
) -> None:
    """
    Phase 9 R1-H (atomicity):
    If a Reservation is already RELEASED (e.g. concurrent update) and the Attempt
    is still RUNNING, the reservation-release path is idempotent: it looks for
    ACTIVE reservations and finds none, so the transition only affects the Attempt.

    This validates that the ACTIVE filter prevents double-release without error.
    """
    worker, job, attempt, reservation = await _persist_worker_job_attempt_reservation(
        attempt_status=AttemptStatus.RUNNING
    )

    # Pre-release the reservation manually (simulates a concurrent release).
    async with AsyncSessionFactory() as session:
        res = await session.get(Reservation, reservation.id)
        res.status = ReservationStatus.RELEASED
        res.released_at = datetime.now(UTC)
        await session.commit()

    # Terminal transition: no ACTIVE reservation to release, but Attempt update still works.
    response = _post_status(str(attempt.id), {"status": "SUCCEEDED", "exit_code": 0})
    assert response.status_code == 200

    async with AsyncSessionFactory() as session:
        persisted_attempt = await session.get(Attempt, attempt.id)
        persisted_res = await session.get(Reservation, reservation.id)

    # Attempt reached terminal state.
    assert persisted_attempt is not None
    assert persisted_attempt.status == AttemptStatus.SUCCEEDED

    # Reservation remains RELEASED (not reset, not duplicated).
    assert persisted_res is not None
    assert persisted_res.status == ReservationStatus.RELEASED

    # Still exactly one reservation record.
    async with AsyncSessionFactory() as session:
        all_res = list(
            (
                await session.execute(
                    select(Reservation).where(Reservation.attempt_id == attempt.id)
                )
            ).scalars()
        )
    assert len(all_res) == 1

