import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from forge.api.main import app
from forge.db.config import get_settings
from forge.db.models import Attempt, AttemptStatus, Job, JobStatus, Worker, WorkerStatus
from forge.db.session import AsyncSessionFactory, get_session


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
        await session.execute(delete(Attempt))
        await session.execute(delete(Job))
        await session.execute(delete(Worker))
        await session.commit()


async def _session_override() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionFactory() as session:
        yield session


async def _persist_attempt(status: AttemptStatus) -> Attempt:
    worker = Worker(
        id=uuid4(),
        name=f"attempt-worker-{uuid4().hex}",
        version="0.1.0",
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=0,
        capabilities=["python"],
        status=WorkerStatus.READY,
    )
    job = Job(
        id=uuid4(),
        name=f"attempt-job-{uuid4().hex}",
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
    started_at = (
        datetime.now(UTC)
        if status
        in {
            AttemptStatus.STARTING,
            AttemptStatus.RUNNING,
        }
        else None
    )
    attempt = Attempt(
        id=uuid4(),
        job=job,
        attempt_number=1,
        worker=worker,
        status=status,
        started_at=started_at,
    )
    async with AsyncSessionFactory() as session:
        session.add_all([worker, job, attempt])
        await session.commit()
    return attempt


def _post_status(attempt_id: str, payload: dict[str, object]):
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            return client.post(f"/api/v1/attempts/{attempt_id}/status", json=payload)
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_unknown_attempt_returns_not_found(clean_database: None) -> None:
    response = _post_status(str(uuid4()), {"status": "STARTING"})

    assert response.status_code == 404
    assert response.json() == {"detail": "Attempt not found"}


@pytest.mark.asyncio
async def test_assigned_attempt_becomes_starting(clean_database: None) -> None:
    attempt = await _persist_attempt(AttemptStatus.ASSIGNED)
    response = _post_status(str(attempt.id), {"status": "STARTING"})

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == AttemptStatus.STARTING.value
    assert response_data["started_at"] is not None

    async with AsyncSessionFactory() as session:
        persisted_attempt = await session.get(Attempt, attempt.id)

    assert persisted_attempt is not None
    assert persisted_attempt.status == AttemptStatus.STARTING
    assert persisted_attempt.started_at is not None
    assert datetime.fromisoformat(response_data["started_at"]) == persisted_attempt.started_at


@pytest.mark.asyncio
async def test_starting_attempt_becomes_running_without_overwriting_started_at(
    clean_database: None,
) -> None:
    attempt = await _persist_attempt(AttemptStatus.STARTING)
    original_started_at = attempt.started_at
    response = _post_status(str(attempt.id), {"status": "RUNNING"})

    assert response.status_code == 200
    assert response.json()["status"] == AttemptStatus.RUNNING.value

    async with AsyncSessionFactory() as session:
        persisted_attempt = await session.get(Attempt, attempt.id)

    assert persisted_attempt is not None
    assert persisted_attempt.status == AttemptStatus.RUNNING
    assert persisted_attempt.started_at == original_started_at


@pytest.mark.asyncio
async def test_running_attempt_succeeds_with_exit_code(clean_database: None) -> None:
    attempt = await _persist_attempt(AttemptStatus.RUNNING)
    response = _post_status(
        str(attempt.id),
        {"status": "SUCCEEDED", "exit_code": 0},
    )

    assert response.status_code == 200
    assert response.json()["status"] == AttemptStatus.SUCCEEDED.value

    async with AsyncSessionFactory() as session:
        persisted_attempt = await session.get(Attempt, attempt.id)

    assert persisted_attempt is not None
    assert persisted_attempt.status == AttemptStatus.SUCCEEDED
    assert persisted_attempt.finished_at is not None
    assert persisted_attempt.exit_code == 0


@pytest.mark.asyncio
async def test_running_attempt_fails_with_error_details(clean_database: None) -> None:
    attempt = await _persist_attempt(AttemptStatus.RUNNING)
    response = _post_status(
        str(attempt.id),
        {"status": "FAILED", "exit_code": 1, "error_message": "container failed"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == AttemptStatus.FAILED.value

    async with AsyncSessionFactory() as session:
        persisted_attempt = await session.get(Attempt, attempt.id)

    assert persisted_attempt is not None
    assert persisted_attempt.status == AttemptStatus.FAILED
    assert persisted_attempt.finished_at is not None
    assert persisted_attempt.exit_code == 1
    assert persisted_attempt.error_message == "container failed"


@pytest.mark.asyncio
async def test_running_attempt_becomes_cancelled(clean_database: None) -> None:
    attempt = await _persist_attempt(AttemptStatus.RUNNING)
    response = _post_status(str(attempt.id), {"status": "CANCELLED"})

    assert response.status_code == 200
    assert response.json()["status"] == AttemptStatus.CANCELLED.value

    async with AsyncSessionFactory() as session:
        persisted_attempt = await session.get(Attempt, attempt.id)

    assert persisted_attempt is not None
    assert persisted_attempt.status == AttemptStatus.CANCELLED
    assert persisted_attempt.finished_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("initial_status", "new_status"),
    [
        (AttemptStatus.CREATED, "RUNNING"),
        (AttemptStatus.ASSIGNED, "RUNNING"),
        (AttemptStatus.STARTING, "SUCCEEDED"),
        (AttemptStatus.RUNNING, "STARTING"),
        (AttemptStatus.SUCCEEDED, "RUNNING"),
    ],
)
async def test_invalid_attempt_transition_returns_conflict(
    clean_database: None,
    initial_status: AttemptStatus,
    new_status: str,
) -> None:
    attempt = await _persist_attempt(initial_status)
    original_started_at = attempt.started_at
    response = _post_status(str(attempt.id), {"status": new_status})

    assert response.status_code == 409
    assert "Invalid attempt status transition" in response.json()["detail"]

    async with AsyncSessionFactory() as session:
        persisted_attempt = await session.get(Attempt, attempt.id)

    assert persisted_attempt is not None
    assert persisted_attempt.status == initial_status
    assert persisted_attempt.started_at == original_started_at
    assert persisted_attempt.finished_at is None
    assert persisted_attempt.exit_code is None
    assert persisted_attempt.error_message is None


@pytest.mark.asyncio
async def test_status_update_does_not_cascade_to_related_records(clean_database: None) -> None:
    attempt = await _persist_attempt(AttemptStatus.RUNNING)
    response = _post_status(str(attempt.id), {"status": "SUCCEEDED", "exit_code": 0})

    assert response.status_code == 200

    async with AsyncSessionFactory() as session:
        persisted_attempt = await session.get(Attempt, attempt.id)
        persisted_job = await session.get(Job, attempt.job_id)
        persisted_worker = await session.get(Worker, attempt.worker_id)

    assert persisted_attempt is not None
    assert persisted_job is not None
    assert persisted_worker is not None
    assert persisted_attempt.status == AttemptStatus.SUCCEEDED
    assert persisted_job.status == JobStatus.QUEUED
    assert persisted_worker.status == WorkerStatus.READY
