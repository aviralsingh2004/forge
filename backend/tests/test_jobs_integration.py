import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from forge.api.main import app
from forge.db.config import get_settings
from forge.db.models import Job, JobEvent, JobStatus, OutboxEvent
from forge.db.session import AsyncSessionFactory, get_session
from forge.messaging.redis_streams import RedisStreams


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
        await session.execute(delete(OutboxEvent))
        await session.execute(delete(Job))
        await session.commit()


async def _session_override() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionFactory() as session:
        yield session


def _job_payload(name: str) -> dict[str, object]:
    return {
        "name": name,
        "image": "python:3.12",
        "command": ["python", "-c", "print(1)"],
        "priority": 80,
        "cpu_required": 2,
        "memory_required_mb": 512,
        "gpu_required": 0,
        "required_capabilities": ["python"],
        "max_retries": 3,
    }


async def _persist_job(status: JobStatus = JobStatus.QUEUED) -> Job:
    job = Job(
        id=uuid4(),
        name="retrievable-api-integration-job",
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        priority=65,
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=1,
        required_capabilities=["python", "cuda"],
        status=status,
        max_retries=4,
    )
    async with AsyncSessionFactory() as session:
        session.add(job)
        await session.commit()
    return job


async def _persist_jobs_for_listing() -> list[Job]:
    start = datetime.now(UTC)
    jobs = [
        Job(
            id=uuid4(),
            name="listing-low-priority",
            image="python:3.12",
            command=["python", "low.py"],
            priority=20,
            cpu_required=1,
            memory_required_mb=256,
            gpu_required=0,
            required_capabilities=["python"],
            status=JobStatus.QUEUED,
            max_retries=1,
            created_at=start,
            updated_at=start,
        ),
        Job(
            id=uuid4(),
            name="listing-same-priority-older",
            image="python:3.12",
            command=["python", "older.py"],
            priority=50,
            cpu_required=2,
            memory_required_mb=512,
            gpu_required=0,
            required_capabilities=["python", "batch"],
            status=JobStatus.QUEUED,
            max_retries=2,
            created_at=start + timedelta(seconds=1),
            updated_at=start + timedelta(seconds=1),
        ),
        Job(
            id=uuid4(),
            name="listing-same-priority-newer",
            image="python:3.12",
            command=["python", "newer.py"],
            priority=50,
            cpu_required=4,
            memory_required_mb=1024,
            gpu_required=1,
            required_capabilities=["python", "gpu"],
            status=JobStatus.QUEUED,
            max_retries=3,
            created_at=start + timedelta(seconds=2),
            updated_at=start + timedelta(seconds=2),
        ),
        Job(
            id=uuid4(),
            name="listing-high-priority",
            image="python:3.12",
            command=["python", "high.py"],
            priority=90,
            cpu_required=8,
            memory_required_mb=2048,
            gpu_required=2,
            required_capabilities=["python", "gpu", "urgent"],
            status=JobStatus.QUEUED,
            max_retries=5,
            created_at=start + timedelta(seconds=3),
            updated_at=start + timedelta(seconds=3),
        ),
    ]
    async with AsyncSessionFactory() as session:
        session.add_all(jobs)
        await session.commit()
    return jobs


@pytest.mark.asyncio
async def test_create_job_persists_job_and_outbox_event(
    clean_database: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _job_payload("api-integration-job")
    publish = AsyncMock()
    monkeypatch.setattr(RedisStreams, "publish", publish)
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post("/api/v1/jobs", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 201
    response_data = response.json()
    job_id = UUID(response_data["id"])

    for field, value in payload.items():
        assert response_data[field] == value
    assert response_data["status"] == JobStatus.QUEUED.value
    assert response_data["created_at"]
    assert response_data["updated_at"]

    async with AsyncSessionFactory() as verification_session:
        persisted_job = await verification_session.get(Job, job_id)
        outbox_events = list(
            await verification_session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["job_id"].astext == str(job_id),
                )
            )
        )

    assert persisted_job is not None
    assert persisted_job.id == job_id
    assert persisted_job.name == payload["name"]
    assert persisted_job.status == JobStatus.QUEUED
    assert len(outbox_events) == 1

    outbox_event = outbox_events[0]
    assert outbox_event.event_type == "JOB_CREATED"
    assert outbox_event.published_at is None
    assert outbox_event.retry_count == 0
    assert outbox_event.last_error is None
    assert set(outbox_event.payload) == {"event_id", "event_type", "job_id", "created_at"}
    assert UUID(outbox_event.payload["event_id"])
    assert outbox_event.payload["event_type"] == "JOB_CREATED"
    assert outbox_event.payload["job_id"] == str(job_id)
    assert outbox_event.payload["created_at"]
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_job_rejects_invalid_input_without_persistence(clean_database: None) -> None:
    payload = _job_payload("invalid-api-integration-job")
    payload["cpu_required"] = -1

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post("/api/v1/jobs", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422

    async with AsyncSessionFactory() as verification_session:
        persisted_jobs = list(
            await verification_session.scalars(
                select(Job).where(Job.name == "invalid-api-integration-job")
            )
        )
        outbox_events = list(await verification_session.scalars(select(OutboxEvent)))

    assert persisted_jobs == []
    assert outbox_events == []


@pytest.mark.asyncio
async def test_get_job_returns_persisted_job(clean_database: None) -> None:
    job = await _persist_job()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.get(f"/api/v1/jobs/{job.id}")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    response_data = response.json()
    assert {
        field: response_data[field]
        for field in (
            "id",
            "name",
            "image",
            "command",
            "priority",
            "cpu_required",
            "memory_required_mb",
            "gpu_required",
            "required_capabilities",
            "status",
            "max_retries",
        )
    } == {
        "id": str(job.id),
        "name": job.name,
        "image": job.image,
        "command": job.command,
        "priority": job.priority,
        "cpu_required": job.cpu_required,
        "memory_required_mb": job.memory_required_mb,
        "gpu_required": job.gpu_required,
        "required_capabilities": job.required_capabilities,
        "status": JobStatus.QUEUED.value,
        "max_retries": job.max_retries,
    }
    assert datetime.fromisoformat(response_data["created_at"]) == job.created_at
    assert datetime.fromisoformat(response_data["updated_at"]) == job.updated_at


@pytest.mark.asyncio
async def test_get_job_returns_not_found_for_unknown_job(clean_database: None) -> None:
    unknown_job_id = uuid4()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.get(f"/api/v1/jobs/{unknown_job_id}")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


@pytest.mark.asyncio
async def test_update_job_persists_changes_and_creates_events(
    clean_database: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = await _persist_job()
    original_name = job.name
    original_cpu_required = job.cpu_required
    publish = AsyncMock()
    monkeypatch.setattr(RedisStreams, "publish", publish)

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.patch(
                f"/api/v1/jobs/{job.id}",
                json={"priority": 90, "max_retries": 7},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["id"] == str(job.id)
    assert response_data["priority"] == 90
    assert response_data["max_retries"] == 7
    assert response_data["name"] == original_name
    assert response_data["cpu_required"] == original_cpu_required

    async with AsyncSessionFactory() as verification_session:
        persisted_job = await verification_session.get(Job, job.id)
        job_events = list(
            await verification_session.scalars(select(JobEvent).where(JobEvent.job_id == job.id))
        )
        outbox_events = list(
            await verification_session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["job_id"].astext == str(job.id),
                )
            )
        )

    assert persisted_job is not None
    assert persisted_job.priority == 90
    assert persisted_job.max_retries == 7
    assert persisted_job.name == original_name
    assert persisted_job.cpu_required == original_cpu_required

    assert len(job_events) == 1
    assert job_events[0].event_type == "JOB_UPDATED"
    assert job_events[0].event_metadata == {"updated_fields": ["priority", "max_retries"]}

    assert len(outbox_events) == 1
    outbox_event = outbox_events[0]
    assert outbox_event.event_type == "JOB_UPDATED"
    assert outbox_event.published_at is None
    assert outbox_event.retry_count == 0
    assert set(outbox_event.payload) == {"event_id", "event_type", "job_id", "created_at"}
    assert UUID(outbox_event.payload["event_id"])
    assert outbox_event.payload["event_type"] == "JOB_UPDATED"
    assert outbox_event.payload["job_id"] == str(job.id)
    assert outbox_event.payload["created_at"]
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_job_rejects_empty_patch_without_changes(clean_database: None) -> None:
    job = await _persist_job()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.patch(f"/api/v1/jobs/{job.id}", json={})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json() == {"detail": "No fields to update"}

    async with AsyncSessionFactory() as verification_session:
        persisted_job = await verification_session.get(Job, job.id)
        job_events = list(
            await verification_session.scalars(select(JobEvent).where(JobEvent.job_id == job.id))
        )
        outbox_events = list(
            await verification_session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["job_id"].astext == str(job.id),
                )
            )
        )

    assert persisted_job is not None
    assert persisted_job.priority == job.priority
    assert persisted_job.max_retries == job.max_retries
    assert job_events == []
    assert outbox_events == []


@pytest.mark.asyncio
async def test_update_unknown_job_returns_not_found(clean_database: None) -> None:
    unknown_job_id = uuid4()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.patch(
                f"/api/v1/jobs/{unknown_job_id}",
                json={"priority": 90},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}

    async with AsyncSessionFactory() as verification_session:
        job_events = list(
            await verification_session.scalars(
                select(JobEvent).where(JobEvent.job_id == unknown_job_id)
            )
        )
        outbox_events = list(
            await verification_session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["job_id"].astext == str(unknown_job_id),
                )
            )
        )

    assert job_events == []
    assert outbox_events == []


@pytest.mark.asyncio
async def test_update_job_rejects_invalid_value_without_changes(clean_database: None) -> None:
    job = await _persist_job()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.patch(
                f"/api/v1/jobs/{job.id}",
                json={"priority": -1},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422

    async with AsyncSessionFactory() as verification_session:
        persisted_job = await verification_session.get(Job, job.id)
        job_events = list(
            await verification_session.scalars(select(JobEvent).where(JobEvent.job_id == job.id))
        )
        outbox_events = list(
            await verification_session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["job_id"].astext == str(job.id),
                )
            )
        )

    assert persisted_job is not None
    assert persisted_job.priority == job.priority
    assert persisted_job.max_retries == job.max_retries
    assert job_events == []
    assert outbox_events == []


@pytest.mark.asyncio
async def test_list_jobs_returns_ordered_jobs_and_all_response_fields(
    clean_database: None,
) -> None:
    jobs = await _persist_jobs_for_listing()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.get("/api/v1/jobs")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    response_data = response.json()
    expected_jobs = [jobs[3], jobs[1], jobs[2], jobs[0]]
    assert [item["id"] for item in response_data] == [str(job.id) for job in expected_jobs]

    for item, job in zip(response_data, expected_jobs, strict=True):
        assert {
            field: item[field]
            for field in (
                "id",
                "name",
                "image",
                "command",
                "priority",
                "cpu_required",
                "memory_required_mb",
                "gpu_required",
                "required_capabilities",
                "status",
                "max_retries",
            )
        } == {
            "id": str(job.id),
            "name": job.name,
            "image": job.image,
            "command": job.command,
            "priority": job.priority,
            "cpu_required": job.cpu_required,
            "memory_required_mb": job.memory_required_mb,
            "gpu_required": job.gpu_required,
            "required_capabilities": job.required_capabilities,
            "status": JobStatus.QUEUED.value,
            "max_retries": job.max_retries,
        }
        assert datetime.fromisoformat(item["created_at"]) == job.created_at
        assert datetime.fromisoformat(item["updated_at"]) == job.updated_at


@pytest.mark.asyncio
async def test_list_jobs_returns_empty_list_when_no_jobs_exist(clean_database: None) -> None:
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.get("/api/v1/jobs")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_cancel_queued_job_persists_status_and_events(
    clean_database: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = await _persist_job()
    publish = AsyncMock()
    monkeypatch.setattr(RedisStreams, "publish", publish)

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(f"/api/v1/jobs/{job.id}/cancel")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["id"] == str(job.id)
    assert response_data["status"] == JobStatus.CANCELLED.value

    async with AsyncSessionFactory() as verification_session:
        persisted_job = await verification_session.get(Job, job.id)
        job_events = list(
            await verification_session.scalars(
                select(JobEvent).where(
                    JobEvent.job_id == job.id,
                    JobEvent.event_type == "JOB_CANCELLED",
                )
            )
        )
        outbox_events = list(
            await verification_session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["job_id"].astext == str(job.id),
                    OutboxEvent.event_type == "JOB_CANCELLED",
                )
            )
        )

    assert persisted_job is not None
    assert persisted_job.status == JobStatus.CANCELLED
    assert len(job_events) == 1
    assert job_events[0].event_type == "JOB_CANCELLED"
    assert len(outbox_events) == 1

    outbox_event = outbox_events[0]
    assert outbox_event.event_type == "JOB_CANCELLED"
    assert outbox_event.published_at is None
    assert outbox_event.payload["job_id"] == str(job.id)
    assert UUID(outbox_event.payload["event_id"])
    assert outbox_event.payload["created_at"]
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_unknown_job_returns_not_found_without_events(clean_database: None) -> None:
    unknown_job_id = uuid4()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(f"/api/v1/jobs/{unknown_job_id}/cancel")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}

    async with AsyncSessionFactory() as verification_session:
        job_events = list(
            await verification_session.scalars(
                select(JobEvent).where(JobEvent.job_id == unknown_job_id)
            )
        )
        outbox_events = list(
            await verification_session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["job_id"].astext == str(unknown_job_id),
                )
            )
        )

    assert job_events == []
    assert outbox_events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_status",
    [
        JobStatus.ASSIGNED,
        JobStatus.RUNNING,
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    ],
)
async def test_cancel_non_queued_job_returns_conflict_without_events(
    clean_database: None,
    job_status: JobStatus,
) -> None:
    job = await _persist_job(status=job_status)
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(f"/api/v1/jobs/{job.id}/cancel")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 409
    assert response.json() == {"detail": "Only queued jobs can be cancelled"}

    async with AsyncSessionFactory() as verification_session:
        persisted_job = await verification_session.get(Job, job.id)
        job_events = list(
            await verification_session.scalars(
                select(JobEvent).where(
                    JobEvent.job_id == job.id,
                    JobEvent.event_type == "JOB_CANCELLED",
                )
            )
        )
        outbox_events = list(
            await verification_session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["job_id"].astext == str(job.id),
                    OutboxEvent.event_type == "JOB_CANCELLED",
                )
            )
        )

    assert persisted_job is not None
    assert persisted_job.status == job_status
    assert job_events == []
    assert outbox_events == []
