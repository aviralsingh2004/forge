import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from forge.api.main import app
from forge.db.config import get_settings
from forge.db.models import (
    Assignment,
    AssignmentStatus,
    Attempt,
    AttemptStatus,
    Job,
    JobStatus,
    Worker,
    WorkerHeartbeat,
    WorkerStatus,
)
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
        await session.execute(delete(WorkerHeartbeat))
        await session.execute(delete(Assignment))
        await session.execute(delete(Attempt))
        await session.execute(delete(Job))
        await session.execute(delete(Worker))
        await session.commit()


async def _session_override() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionFactory() as session:
        yield session


def _worker_payload(name: str = "integration-worker") -> dict[str, object]:
    return {
        "name": name,
        "version": "0.1.0",
        "cpu_capacity": 8,
        "memory_capacity_mb": 16384,
        "gpu_capacity": 1,
        "capabilities": ["docker", "python"],
    }


async def _persist_worker(name: str = "heartbeat-worker") -> Worker:
    worker = Worker(
        id=uuid4(),
        name=name,
        version="0.1.0",
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=1,
        capabilities=["docker", "python"],
        status=WorkerStatus.REGISTERING,
    )
    async with AsyncSessionFactory() as session:
        session.add(worker)
        await session.commit()
    return worker


async def _persist_assignment(
    *,
    worker: Worker | None = None,
    status: AssignmentStatus = AssignmentStatus.CREATED,
) -> tuple[Worker, Attempt, Assignment]:
    worker = worker or await _persist_worker(f"assignment-worker-{uuid4().hex}")
    job = Job(
        id=uuid4(),
        name=f"assignment-job-{uuid4().hex}",
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
        status=AttemptStatus.CREATED,
    )
    assignment = Assignment(
        id=uuid4(),
        attempt=attempt,
        worker=worker,
        status=status,
        delivered_at=datetime.now(UTC) if status != AssignmentStatus.CREATED else None,
    )
    async with AsyncSessionFactory() as session:
        session.add_all([job, attempt, assignment])
        await session.commit()
    return worker, attempt, assignment


@pytest.mark.asyncio
async def test_register_worker_persists_worker(clean_database: None) -> None:
    payload = _worker_payload()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post("/api/v1/workers/register", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 201
    response_data = response.json()
    worker_id = UUID(response_data["id"])
    assert response_data["id"] == str(worker_id)
    for field, value in payload.items():
        assert response_data[field] == value
    assert response_data["status"] == WorkerStatus.REGISTERING.value
    assert response_data["last_heartbeat_at"] is None

    async with AsyncSessionFactory() as verification_session:
        worker = await verification_session.get(Worker, worker_id)

    assert worker is not None
    assert worker.name == payload["name"]
    assert worker.version == payload["version"]
    assert worker.cpu_capacity == payload["cpu_capacity"]
    assert worker.memory_capacity_mb == payload["memory_capacity_mb"]
    assert worker.gpu_capacity == payload["gpu_capacity"]
    assert worker.capabilities == payload["capabilities"]
    assert worker.status == WorkerStatus.REGISTERING


@pytest.mark.asyncio
async def test_register_worker_duplicate_name_raises_database_error(clean_database: None) -> None:
    payload = _worker_payload("duplicate-worker")
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            first_response = client.post("/api/v1/workers/register", json=payload)
            assert first_response.status_code == 201
            with pytest.raises(IntegrityError):
                client.post("/api/v1/workers/register", json=payload)
    finally:
        app.dependency_overrides.clear()

    async with AsyncSessionFactory() as verification_session:
        workers = list(
            await verification_session.scalars(
                select(Worker).where(Worker.name == "duplicate-worker")
            )
        )

    assert len(workers) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", ""),
        ("version", ""),
        ("cpu_capacity", -1),
        ("memory_capacity_mb", -1),
        ("gpu_capacity", -1),
    ],
)
async def test_register_worker_rejects_invalid_payload(
    clean_database: None,
    field: str,
    value: object,
) -> None:
    payload = _worker_payload(f"invalid-{field}")
    payload[field] = value
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post("/api/v1/workers/register", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422

    async with AsyncSessionFactory() as verification_session:
        workers = list(
            await verification_session.scalars(
                select(Worker).where(Worker.name == f"invalid-{field}")
            )
        )

    assert workers == []


@pytest.mark.asyncio
async def test_register_worker_rejects_missing_required_field(clean_database: None) -> None:
    payload = _worker_payload("invalid-missing-version")
    del payload["version"]
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post("/api/v1/workers/register", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422

    async with AsyncSessionFactory() as verification_session:
        workers = list(
            await verification_session.scalars(
                select(Worker).where(Worker.name == "invalid-missing-version")
            )
        )

    assert workers == []


@pytest.mark.asyncio
async def test_register_worker_defaults_capabilities_to_empty_list(clean_database: None) -> None:
    payload = _worker_payload("worker-with-default-capabilities")
    del payload["capabilities"]
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post("/api/v1/workers/register", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 201
    assert response.json()["capabilities"] == []

    worker_id = UUID(response.json()["id"])
    async with AsyncSessionFactory() as verification_session:
        worker = await verification_session.get(Worker, worker_id)

    assert worker is not None
    assert worker.capabilities == []


@pytest.mark.asyncio
async def test_worker_heartbeat_updates_worker_and_persists_record(
    clean_database: None,
) -> None:
    worker = await _persist_worker()
    attempt_ids = [
        "00000000-0000-0000-0000-000000000011",
        "00000000-0000-0000-0000-000000000012",
    ]
    payload = {
        "cpu_usage": 3,
        "memory_usage_mb": 2048,
        "gpu_usage": 1,
        "running_attempts": attempt_ids,
    }

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(f"/api/v1/workers/{worker.id}/heartbeat", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["worker_id"] == str(worker.id)
    response_timestamp = datetime.fromisoformat(response_data["timestamp"])
    assert response_timestamp.tzinfo is not None

    async with AsyncSessionFactory() as verification_session:
        persisted_worker = await verification_session.get(Worker, worker.id)
        heartbeats = list(
            await verification_session.scalars(
                select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker.id)
            )
        )

    assert persisted_worker is not None
    assert persisted_worker.last_heartbeat_at is not None
    assert persisted_worker.last_heartbeat_at.tzinfo is not None
    assert abs(persisted_worker.last_heartbeat_at - response_timestamp).total_seconds() < 1

    assert len(heartbeats) == 1
    heartbeat = heartbeats[0]
    assert heartbeat.worker_id == worker.id
    assert heartbeat.cpu_usage == payload["cpu_usage"]
    assert heartbeat.memory_usage_mb == payload["memory_usage_mb"]
    assert heartbeat.gpu_usage == payload["gpu_usage"]
    assert heartbeat.running_attempts == attempt_ids
    assert heartbeat.timestamp.tzinfo is not None
    assert abs(heartbeat.timestamp - response_timestamp).total_seconds() < 1


@pytest.mark.asyncio
async def test_worker_heartbeat_defaults_running_attempts_to_empty_list(
    clean_database: None,
) -> None:
    worker = await _persist_worker("heartbeat-default-attempts-worker")
    payload = {"cpu_usage": 1, "memory_usage_mb": 256, "gpu_usage": 0}

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(f"/api/v1/workers/{worker.id}/heartbeat", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200

    async with AsyncSessionFactory() as verification_session:
        heartbeats = list(
            await verification_session.scalars(
                select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker.id)
            )
        )

    assert len(heartbeats) == 1
    assert heartbeats[0].running_attempts == []


@pytest.mark.asyncio
async def test_worker_heartbeat_unknown_worker_returns_not_found(clean_database: None) -> None:
    unknown_worker_id = UUID("00000000-0000-0000-0000-000000000099")
    payload = {"cpu_usage": 1, "memory_usage_mb": 256, "gpu_usage": 0}

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/workers/{unknown_worker_id}/heartbeat",
                json=payload,
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Worker not found"}

    async with AsyncSessionFactory() as verification_session:
        heartbeats = list(
            await verification_session.scalars(
                select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == unknown_worker_id)
            )
        )

    assert heartbeats == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["cpu_usage", "memory_usage_mb", "gpu_usage"])
async def test_worker_heartbeat_rejects_negative_usage(
    clean_database: None,
    field: str,
) -> None:
    worker = await _persist_worker(f"heartbeat-invalid-{field}")
    payload = {"cpu_usage": 1, "memory_usage_mb": 256, "gpu_usage": 0}
    payload[field] = -1

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(f"/api/v1/workers/{worker.id}/heartbeat", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422

    async with AsyncSessionFactory() as verification_session:
        heartbeats = list(
            await verification_session.scalars(
                select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker.id)
            )
        )

    assert heartbeats == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["cpu_usage", "memory_usage_mb", "gpu_usage"])
async def test_worker_heartbeat_rejects_missing_usage_field(
    clean_database: None,
    field: str,
) -> None:
    worker = await _persist_worker(f"heartbeat-missing-{field}")
    payload = {"cpu_usage": 1, "memory_usage_mb": 256, "gpu_usage": 0}
    del payload[field]

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(f"/api/v1/workers/{worker.id}/heartbeat", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422

    async with AsyncSessionFactory() as verification_session:
        heartbeats = list(
            await verification_session.scalars(
                select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker.id)
            )
        )

    assert heartbeats == []


@pytest.mark.asyncio
async def test_worker_heartbeat_persists_multiple_records_and_latest_timestamp(
    clean_database: None,
) -> None:
    worker = await _persist_worker("heartbeat-multiple-worker")
    first_payload = {"cpu_usage": 1, "memory_usage_mb": 256, "gpu_usage": 0}
    second_payload = {
        "cpu_usage": 4,
        "memory_usage_mb": 1024,
        "gpu_usage": 1,
        "running_attempts": ["00000000-0000-0000-0000-000000000021"],
    }

    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            first_response = client.post(
                f"/api/v1/workers/{worker.id}/heartbeat",
                json=first_payload,
            )
            second_response = client.post(
                f"/api/v1/workers/{worker.id}/heartbeat",
                json=second_payload,
            )
    finally:
        app.dependency_overrides.clear()

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    first_timestamp = datetime.fromisoformat(first_response.json()["timestamp"])
    second_timestamp = datetime.fromisoformat(second_response.json()["timestamp"])
    assert second_timestamp >= first_timestamp

    async with AsyncSessionFactory() as verification_session:
        persisted_worker = await verification_session.get(Worker, worker.id)
        heartbeats = list(
            await verification_session.scalars(
                select(WorkerHeartbeat)
                .where(WorkerHeartbeat.worker_id == worker.id)
                .order_by(WorkerHeartbeat.timestamp.asc())
            )
        )

    assert persisted_worker is not None
    assert persisted_worker.last_heartbeat_at is not None
    assert abs(persisted_worker.last_heartbeat_at - second_timestamp).total_seconds() < 1
    assert len(heartbeats) == 2
    assert heartbeats[0].cpu_usage == first_payload["cpu_usage"]
    assert heartbeats[1].cpu_usage == second_payload["cpu_usage"]
    assert heartbeats[1].running_attempts == second_payload["running_attempts"]


@pytest.mark.asyncio
async def test_deliver_assignment_updates_status_and_timestamp(clean_database: None) -> None:
    worker, attempt, assignment = await _persist_assignment()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/workers/{worker.id}/assignments",
                json={"assignment_id": str(assignment.id)},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["id"] == str(assignment.id)
    assert response_data["attempt_id"] == str(attempt.id)
    assert response_data["worker_id"] == str(worker.id)
    assert response_data["status"] == AssignmentStatus.DELIVERED.value
    assert response_data["delivered_at"] is not None

    async with AsyncSessionFactory() as verification_session:
        persisted_assignment = await verification_session.get(Assignment, assignment.id)
        persisted_attempt = await verification_session.get(Attempt, attempt.id)

    assert persisted_assignment is not None
    assert persisted_assignment.status == AssignmentStatus.DELIVERED
    assert persisted_assignment.delivered_at is not None
    assert persisted_assignment.delivered_at.tzinfo is not None
    response_timestamp = datetime.fromisoformat(response_data["delivered_at"])
    assert abs(persisted_assignment.delivered_at - response_timestamp).total_seconds() < 1
    assert persisted_attempt is not None
    assert persisted_attempt.status == AttemptStatus.CREATED


@pytest.mark.asyncio
async def test_deliver_assignment_unknown_worker_returns_not_found(
    clean_database: None,
) -> None:
    worker, _, assignment = await _persist_assignment()
    unknown_worker_id = uuid4()
    assert unknown_worker_id != worker.id
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/workers/{unknown_worker_id}/assignments",
                json={"assignment_id": str(assignment.id)},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Worker not found"}

    async with AsyncSessionFactory() as verification_session:
        persisted_assignment = await verification_session.get(Assignment, assignment.id)

    assert persisted_assignment is not None
    assert persisted_assignment.status == AssignmentStatus.CREATED
    assert persisted_assignment.delivered_at is None


@pytest.mark.asyncio
async def test_deliver_assignment_unknown_assignment_returns_not_found(
    clean_database: None,
) -> None:
    worker = await _persist_worker("unknown-assignment-worker")
    unknown_assignment_id = uuid4()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/workers/{worker.id}/assignments",
                json={"assignment_id": str(unknown_assignment_id)},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Assignment not found"}


@pytest.mark.asyncio
async def test_deliver_assignment_for_other_worker_returns_conflict(
    clean_database: None,
) -> None:
    worker_a, _, assignment = await _persist_assignment()
    worker_b = await _persist_worker("assignment-other-worker")
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/workers/{worker_b.id}/assignments",
                json={"assignment_id": str(assignment.id)},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 409
    assert response.json() == {"detail": "Assignment does not belong to worker"}
    assert worker_a.id != worker_b.id

    async with AsyncSessionFactory() as verification_session:
        persisted_assignment = await verification_session.get(Assignment, assignment.id)

    assert persisted_assignment is not None
    assert persisted_assignment.status == AssignmentStatus.CREATED
    assert persisted_assignment.delivered_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "assignment_status",
    [
        AssignmentStatus.DELIVERED,
        AssignmentStatus.ACKNOWLEDGED,
        AssignmentStatus.COMPLETED,
        AssignmentStatus.FAILED,
        AssignmentStatus.CANCELLED,
    ],
)
async def test_deliver_assignment_requires_created_state(
    clean_database: None,
    assignment_status: AssignmentStatus,
) -> None:
    worker, _, assignment = await _persist_assignment(status=assignment_status)
    original_delivered_at = assignment.delivered_at
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/workers/{worker.id}/assignments",
                json={"assignment_id": str(assignment.id)},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 409
    assert response.json() == {"detail": "Assignment is not in CREATED state"}

    async with AsyncSessionFactory() as verification_session:
        persisted_assignment = await verification_session.get(Assignment, assignment.id)

    assert persisted_assignment is not None
    assert persisted_assignment.status == assignment_status
    assert persisted_assignment.delivered_at == original_delivered_at


@pytest.mark.asyncio
async def test_deliver_assignment_only_succeeds_once(clean_database: None) -> None:
    worker, _, assignment = await _persist_assignment()
    app.dependency_overrides[get_session] = _session_override
    try:
        with TestClient(app) as client:
            first_response = client.post(
                f"/api/v1/workers/{worker.id}/assignments",
                json={"assignment_id": str(assignment.id)},
            )
            second_response = client.post(
                f"/api/v1/workers/{worker.id}/assignments",
                json={"assignment_id": str(assignment.id)},
            )
    finally:
        app.dependency_overrides.clear()

    assert first_response.status_code == 200
    assert second_response.status_code == 409
    assert second_response.json() == {"detail": "Assignment is not in CREATED state"}
    first_delivered_at = datetime.fromisoformat(first_response.json()["delivered_at"])

    async with AsyncSessionFactory() as verification_session:
        persisted_assignment = await verification_session.get(Assignment, assignment.id)

    assert persisted_assignment is not None
    assert persisted_assignment.status == AssignmentStatus.DELIVERED
    assert persisted_assignment.delivered_at is not None
    assert abs(persisted_assignment.delivered_at - first_delivered_at).total_seconds() < 1
