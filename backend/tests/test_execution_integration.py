import asyncio
import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import UUID, uuid4

import docker
import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from forge.api.main import app
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
    WorkerHeartbeat,
    WorkerStatus,
)
from forge.db.session import get_session
from forge.scheduler.service import Scheduler
from forge.worker.assignment import AssignmentHandler
from forge.worker.assignment_details import AssignmentDetailsClient
from forge.worker.assignment_execution import AssignmentExecutor
from forge.worker.executor.docker import DockerExecutor
from forge.worker.lifecycle import WorkerExecutionLifecycle
from forge.worker.request_builder import ExecutionRequestBuilder
from forge.worker.status import AttemptStatusReporter

_original_async_client = httpx.AsyncClient


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
        await session.execute(delete(WorkerHeartbeat))
        await session.execute(delete(Assignment))
        await session.execute(delete(Reservation))
        await session.execute(delete(Attempt))
        await session.execute(delete(Job))
        await session.execute(delete(Worker))
        await session.commit()
    yield
    async with session_factory() as session:
        await session.execute(delete(JobEvent))
        await session.execute(delete(WorkerHeartbeat))
        await session.execute(delete(Assignment))
        await session.execute(delete(Reservation))
        await session.execute(delete(Attempt))
        await session.execute(delete(Job))
        await session.execute(delete(Worker))
        await session.commit()


@pytest.fixture
def mock_httpx_asgi(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async def _session_override() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _session_override
    transport = httpx.ASGITransport(app=app)

    def _client_factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
        kwargs_copy = dict(kwargs)
        kwargs_copy.setdefault("transport", transport)
        kwargs_copy.setdefault("base_url", "http://testserver")
        return _original_async_client(*args, **kwargs_copy)

    monkeypatch.setattr("forge.worker.assignment_details.httpx.AsyncClient", _client_factory)
    monkeypatch.setattr("forge.worker.assignment.httpx.AsyncClient", _client_factory)
    monkeypatch.setattr("forge.worker.status.httpx.AsyncClient", _client_factory)

    yield

    app.dependency_overrides.clear()


async def _create_worker_and_job(
    session: AsyncSession,
) -> tuple[Worker, Job]:
    worker = Worker(
        id=uuid4(),
        name=f"exec-worker-{uuid4().hex[:8]}",
        version="0.1.0",
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=0,
        capabilities=["docker", "python"],
        status=WorkerStatus.READY,
    )
    job = Job(
        id=uuid4(),
        name=f"exec-job-{uuid4().hex[:8]}",
        image="python:3.12-slim",
        command=["python", "-c", "print('hello from worker')"],
        priority=100,
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=0,
        required_capabilities=["python"],
        status=JobStatus.QUEUED,
        max_retries=1,
    )
    session.add_all([worker, job])
    await session.commit()
    return worker, job


@pytest.mark.asyncio
async def test_scheduler_assignment_to_worker_execution_success_flow_db(
    clean_database: None,
    session_factory: async_sessionmaker[AsyncSession],
    mock_httpx_asgi: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 1. Setup schedulable Job and READY Worker
    async with session_factory() as session:
        worker, job = await _create_worker_and_job(session)

    # 2. Scheduler transaction: creates Attempt, Reservation, Assignment, JOB_ASSIGNED
    async with session_factory() as session:
        scheduler = Scheduler(session)
        created_assignment = await scheduler.schedule_job(job, worker)
        await session.commit()

        assert created_assignment.status == AssignmentStatus.CREATED
        assert created_assignment.worker_id == worker.id

    # Verify scheduling state in DB
    async with session_factory() as session:
        job_db = await session.get(Job, job.id)
        assert job_db is not None
        assert job_db.status == JobStatus.ASSIGNED

        attempt_res = await session.execute(
            select(Attempt).where(Attempt.job_id == job.id)
        )
        attempt = attempt_res.scalar_one()
        assert attempt.status == AttemptStatus.CREATED
        assert attempt.worker_id == worker.id
        assert attempt.attempt_number == 1

        res_res = await session.execute(
            select(Reservation).where(Reservation.attempt_id == attempt.id)
        )
        reservation = res_res.scalar_one()
        assert reservation.status == ReservationStatus.ACTIVE
        assert reservation.cpu_reserved == 2
        assert reservation.memory_reserved_mb == 1024
        assert reservation.worker_id == worker.id

        assign_res = await session.execute(
            select(Assignment).where(Assignment.attempt_id == attempt.id)
        )
        assignment = assign_res.scalar_one()
        assert assignment.status == AssignmentStatus.CREATED
        assert assignment.worker_id == worker.id

        event_res = await session.execute(
            select(JobEvent).where(JobEvent.job_id == job.id)
        )
        event = event_res.scalar_one()
        assert event.event_type == "JOB_ASSIGNED"
        assert event.attempt_id == attempt.id

    # 3. Deliver Assignment via Control Plane API
    async with _original_async_client(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        delivery_resp = await client.post(
            f"/api/v1/workers/{worker.id}/assignments",
            json={"assignment_id": str(assignment.id)},
        )
        assert delivery_resp.status_code == 200
        assert delivery_resp.json()["status"] == AssignmentStatus.DELIVERED.value

    # 4. Mock Docker SDK container execution boundary
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    container.logs.return_value = b""

    containers_mock = Mock()
    containers_mock.create.return_value = container

    docker_client = Mock()
    docker_client.containers = containers_mock

    monkeypatch.setattr(
        "forge.worker.executor.docker.docker.from_env",
        Mock(return_value=docker_client),
    )

    # 5. Worker execution pipeline components
    details_client = AssignmentDetailsClient("http://testserver", worker.id)
    request_builder = ExecutionRequestBuilder()
    assignment_handler = AssignmentHandler("http://testserver", worker.id)
    docker_executor = DockerExecutor()
    status_reporter = AttemptStatusReporter("http://testserver")
    lifecycle = WorkerExecutionLifecycle(docker_executor, status_reporter)
    assignment_executor = AssignmentExecutor(
        details_client=details_client,
        request_builder=request_builder,
        assignment_handler=assignment_handler,
        lifecycle=lifecycle,
    )

    # 6. Execute assignment through the worker pipeline
    result = await assignment_executor.execute(assignment.id)

    # 7. Assert execution result
    assert result.exit_code == 0
    assert result.error_message is None

    # Verify Docker container lifecycle invocations
    containers_mock.create.assert_called_once_with(
        image="python:3.12-slim",
        command=["python", "-c", "print('hello from worker')"],
        nano_cpus=2_000_000_000,
        mem_limit="1024m",
        device_requests=None,
    )
    container.start.assert_called_once_with()
    container.wait.assert_called_once_with()
    container.remove.assert_called_once_with()

    # 8. Verify final state across the control plane / database
    async with session_factory() as session:
        # Attempt reached terminal SUCCEEDED status with timestamps and exit code
        final_attempt = await session.get(Attempt, attempt.id)
        assert final_attempt is not None
        assert final_attempt.status == AttemptStatus.SUCCEEDED
        assert final_attempt.started_at is not None
        assert final_attempt.finished_at is not None
        assert final_attempt.exit_code == 0
        assert final_attempt.error_message is None

        # Assignment reached ACKNOWLEDGED status
        final_assignment = await session.get(Assignment, assignment.id)
        assert final_assignment is not None
        assert final_assignment.status == AssignmentStatus.ACKNOWLEDGED
        assert final_assignment.delivered_at is not None
        assert final_assignment.acknowledged_at is not None

        # Phase 9 R1: Reservation is RELEASED after SUCCEEDED terminal state.
        final_reservation = await session.get(Reservation, reservation.id)
        assert final_reservation is not None
        assert final_reservation.status == ReservationStatus.RELEASED
        assert final_reservation.released_at is not None


        # Job remains ASSIGNED
        final_job = await session.get(Job, job.id)
        assert final_job is not None
        assert final_job.status == JobStatus.ASSIGNED


@pytest.mark.asyncio
async def test_scheduler_assignment_to_worker_execution_failure_flow_db(
    clean_database: None,
    session_factory: async_sessionmaker[AsyncSession],
    mock_httpx_asgi: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 1. Setup schedulable Job and READY Worker
    async with session_factory() as session:
        worker, job = await _create_worker_and_job(session)

    # 2. Scheduler transaction
    async with session_factory() as session:
        scheduler = Scheduler(session)
        created_assignment = await scheduler.schedule_job(job, worker)
        await session.commit()
        assert created_assignment.status == AssignmentStatus.CREATED

    # Retrieve created Attempt and Assignment
    async with session_factory() as session:
        attempt_res = await session.execute(
            select(Attempt).where(Attempt.job_id == job.id)
        )
        attempt = attempt_res.scalar_one()

        assign_res = await session.execute(
            select(Assignment).where(Assignment.attempt_id == attempt.id)
        )
        assignment = assign_res.scalar_one()

    # 3. Deliver Assignment
    async with _original_async_client(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        delivery_resp = await client.post(
            f"/api/v1/workers/{worker.id}/assignments",
            json={"assignment_id": str(assignment.id)},
        )
        assert delivery_resp.status_code == 200

    # 4. Mock Docker SDK container execution failure
    container = Mock()
    container.wait.return_value = {"StatusCode": 137}
    container.logs.return_value = b"FATAL: Out of memory\n"

    containers_mock = Mock()
    containers_mock.create.return_value = container

    docker_client = Mock()
    docker_client.containers = containers_mock

    monkeypatch.setattr(
        "forge.worker.executor.docker.docker.from_env",
        Mock(return_value=docker_client),
    )

    # 5. Worker execution pipeline components
    details_client = AssignmentDetailsClient("http://testserver", worker.id)
    request_builder = ExecutionRequestBuilder()
    assignment_handler = AssignmentHandler("http://testserver", worker.id)
    docker_executor = DockerExecutor()
    status_reporter = AttemptStatusReporter("http://testserver")
    lifecycle = WorkerExecutionLifecycle(docker_executor, status_reporter)
    assignment_executor = AssignmentExecutor(
        details_client=details_client,
        request_builder=request_builder,
        assignment_handler=assignment_handler,
        lifecycle=lifecycle,
    )

    # 6. Execute assignment through the worker pipeline
    result = await assignment_executor.execute(assignment.id)

    # 7. Assert execution result captures failure details
    assert result.exit_code == 137
    assert result.error_message == "FATAL: Out of memory\n"

    # Container cleanup is guaranteed
    container.remove.assert_called_once_with()

    # 8. Verify final state across the control plane / database
    async with session_factory() as session:
        # Attempt reached terminal FAILED status with failure details
        final_attempt = await session.get(Attempt, attempt.id)
        assert final_attempt is not None
        assert final_attempt.status == AttemptStatus.FAILED
        assert final_attempt.started_at is not None
        assert final_attempt.finished_at is not None
        assert final_attempt.exit_code == 137
        assert final_attempt.error_message == "FATAL: Out of memory\n"

        # Assignment reached ACKNOWLEDGED status
        final_assignment = await session.get(Assignment, assignment.id)
        assert final_assignment is not None
        assert final_assignment.status == AssignmentStatus.ACKNOWLEDGED


@pytest.mark.asyncio
async def test_scheduler_assignment_to_real_docker_execution_db(
    clean_database: None,
    session_factory: async_sessionmaker[AsyncSession],
    mock_httpx_asgi: None,
) -> None:
    try:
        real_client = docker.from_env()
        if not real_client.ping():
            pytest.skip("Docker daemon is not responding to ping")
    except Exception as exc:
        pytest.skip(f"Docker daemon is not available: {exc}")

    # Ensure alpine image is available
    image_name = "alpine:latest"
    try:
        real_client.images.get(image_name)
    except docker.errors.ImageNotFound:
        try:
            real_client.images.pull(image_name)
        except Exception as exc:
            pytest.skip(f"Could not pull {image_name}: {exc}")

    async with session_factory() as session:
        worker = Worker(
            id=uuid4(),
            name=f"real-docker-worker-{uuid4().hex[:8]}",
            version="0.1.0",
            cpu_capacity=4,
            memory_capacity_mb=4096,
            gpu_capacity=0,
            capabilities=["docker", "sh"],
            status=WorkerStatus.READY,
        )
        job = Job(
            id=uuid4(),
            name=f"real-docker-job-{uuid4().hex[:8]}",
            image=image_name,
            command=["echo", "forge real docker execution test"],
            priority=50,
            cpu_required=1,
            memory_required_mb=256,
            gpu_required=0,
            required_capabilities=["sh"],
            status=JobStatus.QUEUED,
            max_retries=1,
        )
        session.add_all([worker, job])
        await session.commit()

    async with session_factory() as session:
        scheduler = Scheduler(session)
        await scheduler.schedule_job(job, worker)
        await session.commit()

    async with session_factory() as session:
        attempt_res = await session.execute(
            select(Attempt).where(Attempt.job_id == job.id)
        )
        attempt = attempt_res.scalar_one()

        assign_res = await session.execute(
            select(Assignment).where(Assignment.attempt_id == attempt.id)
        )
        assignment = assign_res.scalar_one()

    async with _original_async_client(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        delivery_resp = await client.post(
            f"/api/v1/workers/{worker.id}/assignments",
            json={"assignment_id": str(assignment.id)},
        )
        assert delivery_resp.status_code == 200

    # Real DockerExecutor without mocking
    details_client = AssignmentDetailsClient("http://testserver", worker.id)
    request_builder = ExecutionRequestBuilder()
    assignment_handler = AssignmentHandler("http://testserver", worker.id)
    docker_executor = DockerExecutor()
    status_reporter = AttemptStatusReporter("http://testserver")
    lifecycle = WorkerExecutionLifecycle(docker_executor, status_reporter)
    assignment_executor = AssignmentExecutor(
        details_client=details_client,
        request_builder=request_builder,
        assignment_handler=assignment_handler,
        lifecycle=lifecycle,
    )

    result = await assignment_executor.execute(assignment.id)

    assert result.exit_code == 0
    assert result.error_message is None

    async with session_factory() as session:
        final_attempt = await session.get(Attempt, attempt.id)
        assert final_attempt is not None
        assert final_attempt.status == AttemptStatus.SUCCEEDED
        assert final_attempt.started_at is not None
        assert final_attempt.finished_at is not None
        assert final_attempt.exit_code == 0
