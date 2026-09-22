from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from forge.worker.registration import WorkerRegistration
from forge.worker.runtime import create_worker
from forge.worker.worker import Worker

WORKER_ID = UUID("00000000-0000-0000-0000-000000000001")


class FakeRegistrar:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.register = AsyncMock(return_value={"id": str(WORKER_ID)})


class RecordingIdClient:
    instances: list["RecordingIdClient"] = []

    def __init__(self, base_url: str, worker_id: UUID) -> None:
        self.base_url = base_url
        self.worker_id = worker_id
        self.instances.append(self)


class RecordingHeartbeatLoop:
    instances: list["RecordingHeartbeatLoop"] = []

    def __init__(self, heartbeat: object, interval_seconds: float, data_provider: object) -> None:
        self.heartbeat = heartbeat
        self.interval_seconds = interval_seconds
        self.data_provider = data_provider
        self.instances.append(self)


class RecordingHeartbeat:
    instances: list["RecordingHeartbeat"] = []

    def __init__(self, base_url: str, worker_id: UUID) -> None:
        self.base_url = base_url
        self.worker_id = worker_id
        self.instances.append(self)


class RecordingAssignmentHandler:
    instances: list["RecordingAssignmentHandler"] = []

    def __init__(self, base_url: str, worker_id: UUID) -> None:
        self.base_url = base_url
        self.worker_id = worker_id
        self.instances.append(self)


class RecordingDockerExecutor:
    instances: list["RecordingDockerExecutor"] = []

    def __init__(self) -> None:
        self.instances.append(self)


class RecordingStatusReporter:
    instances: list["RecordingStatusReporter"] = []

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.instances.append(self)


class RecordingLifecycle:
    instances: list["RecordingLifecycle"] = []

    def __init__(self, executor: object, status_reporter: object) -> None:
        self.executor = executor
        self.status_reporter = status_reporter
        self.instances.append(self)


class RecordingRequestBuilder:
    instances: list["RecordingRequestBuilder"] = []

    def __init__(self) -> None:
        self.instances.append(self)


class RecordingAssignmentExecutor:
    instances: list["RecordingAssignmentExecutor"] = []

    def __init__(
        self,
        details_client: object,
        request_builder: object,
        assignment_handler: object,
        lifecycle: object,
    ) -> None:
        self.details_client = details_client
        self.request_builder = request_builder
        self.assignment_handler = assignment_handler
        self.lifecycle = lifecycle
        self.instances.append(self)


@pytest.fixture
def registration() -> WorkerRegistration:
    return WorkerRegistration(
        name="worker-1",
        version="0.1.0",
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=1,
        capabilities=["docker", "python"],
    )


@pytest.fixture(autouse=True)
def clear_recorders() -> None:
    for recorder in (
        RecordingIdClient,
        RecordingHeartbeatLoop,
        RecordingHeartbeat,
        RecordingAssignmentHandler,
        RecordingDockerExecutor,
        RecordingStatusReporter,
        RecordingLifecycle,
        RecordingRequestBuilder,
        RecordingAssignmentExecutor,
    ):
        recorder.instances.clear()


def patch_runtime_components(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("forge.worker.runtime.WorkerRegistrar", FakeRegistrar)
    monkeypatch.setattr(
        "forge.worker.runtime.AssignmentDetailsClient",
        RecordingIdClient,
    )
    monkeypatch.setattr("forge.worker.runtime.AssignmentHandler", RecordingAssignmentHandler)
    monkeypatch.setattr("forge.worker.runtime.WorkerHeartbeat", RecordingHeartbeat)
    monkeypatch.setattr("forge.worker.runtime.DockerExecutor", RecordingDockerExecutor)
    monkeypatch.setattr("forge.worker.runtime.AttemptStatusReporter", RecordingStatusReporter)
    monkeypatch.setattr("forge.worker.runtime.WorkerExecutionLifecycle", RecordingLifecycle)
    monkeypatch.setattr("forge.worker.runtime.ExecutionRequestBuilder", RecordingRequestBuilder)
    monkeypatch.setattr("forge.worker.runtime.AssignmentExecutor", RecordingAssignmentExecutor)
    monkeypatch.setattr("forge.worker.runtime.HeartbeatLoop", RecordingHeartbeatLoop)


@pytest.mark.asyncio
async def test_create_worker_successful_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    patch_runtime_components(monkeypatch)
    data_provider = AsyncMock()

    worker = await create_worker(
        base_url="http://localhost:8000",
        registration=registration,
        heartbeat_interval_seconds=10.0,
        data_provider=data_provider,
    )

    assert isinstance(worker, Worker)
    assert worker.id == WORKER_ID
    assert worker.assignment_executor is not None
    assert worker.assignment_executor is RecordingAssignmentExecutor.instances[0]
    assert worker.heartbeat_loop is not None
    assert worker.heartbeat_loop is RecordingHeartbeatLoop.instances[0]


@pytest.mark.asyncio
async def test_create_worker_propagates_registered_worker_id(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    patch_runtime_components(monkeypatch)
    custom_worker_id = UUID("11111111-2222-3333-4444-555555555555")

    class CustomRegistrar(FakeRegistrar):
        def __init__(self, base_url: str) -> None:
            self.base_url = base_url
            self.register = AsyncMock(return_value={"id": str(custom_worker_id)})

    monkeypatch.setattr("forge.worker.runtime.WorkerRegistrar", CustomRegistrar)

    worker = await create_worker(
        base_url="http://localhost:8000",
        registration=registration,
        heartbeat_interval_seconds=10.0,
        data_provider=object(),
    )

    assert worker.id == custom_worker_id
    assert RecordingIdClient.instances[0].worker_id == custom_worker_id
    assert RecordingAssignmentHandler.instances[0].worker_id == custom_worker_id
    assert RecordingHeartbeat.instances[0].worker_id == custom_worker_id


@pytest.mark.asyncio
async def test_create_worker_registers_before_constructing_id_dependent_components(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    events: list[str] = []
    registered_id: UUID | None = None

    class OrderedRegistrar(FakeRegistrar):
        def __init__(self, base_url: str) -> None:
            self.base_url = base_url

        async def register(self, value: WorkerRegistration) -> dict[str, str]:
            nonlocal registered_id
            registered_id = WORKER_ID
            events.append("register")
            return {"id": str(WORKER_ID)}

    class OrderedDetailsClient:
        def __init__(self, base_url: str, worker_id: UUID) -> None:
            assert registered_id is not None, (
                "AssignmentDetailsClient constructed before registration completed"
            )
            assert worker_id == registered_id
            events.append("details_client")

    class OrderedAssignmentHandler:
        def __init__(self, base_url: str, worker_id: UUID) -> None:
            assert registered_id is not None, (
                "AssignmentHandler constructed before registration completed"
            )
            assert worker_id == registered_id
            events.append("assignment_handler")

    class OrderedHeartbeat:
        def __init__(self, base_url: str, worker_id: UUID) -> None:
            assert registered_id is not None, (
                "WorkerHeartbeat constructed before registration completed"
            )
            assert worker_id == registered_id
            events.append("heartbeat")

    monkeypatch.setattr("forge.worker.runtime.WorkerRegistrar", OrderedRegistrar)
    monkeypatch.setattr("forge.worker.runtime.AssignmentDetailsClient", OrderedDetailsClient)
    monkeypatch.setattr("forge.worker.runtime.AssignmentHandler", OrderedAssignmentHandler)
    monkeypatch.setattr("forge.worker.runtime.WorkerHeartbeat", OrderedHeartbeat)
    monkeypatch.setattr("forge.worker.runtime.DockerExecutor", RecordingDockerExecutor)
    monkeypatch.setattr("forge.worker.runtime.AttemptStatusReporter", RecordingStatusReporter)
    monkeypatch.setattr("forge.worker.runtime.WorkerExecutionLifecycle", RecordingLifecycle)
    monkeypatch.setattr("forge.worker.runtime.ExecutionRequestBuilder", RecordingRequestBuilder)
    monkeypatch.setattr("forge.worker.runtime.AssignmentExecutor", RecordingAssignmentExecutor)
    monkeypatch.setattr("forge.worker.runtime.HeartbeatLoop", RecordingHeartbeatLoop)

    await create_worker("http://localhost:8000", registration, 10.0, object())

    assert events[0] == "register"
    assert "details_client" in events[1:]
    assert "assignment_handler" in events[1:]
    assert "heartbeat" in events[1:]


@pytest.mark.asyncio
async def test_create_worker_passes_heartbeat_configuration(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    patch_runtime_components(monkeypatch)
    data_provider = object()

    await create_worker("http://localhost:8000", registration, 12.5, data_provider)

    heartbeat_loop = RecordingHeartbeatLoop.instances[0]
    assert heartbeat_loop.interval_seconds == 12.5
    assert heartbeat_loop.data_provider is data_provider
    assert heartbeat_loop.heartbeat is RecordingHeartbeat.instances[0]


@pytest.mark.asyncio
async def test_create_worker_raises_value_error_for_invalid_uuid(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    class InvalidUUIDRegistrar(FakeRegistrar):
        def __init__(self, base_url: str) -> None:
            self.register = AsyncMock(return_value={"id": "invalid-uuid-format"})

    monkeypatch.setattr("forge.worker.runtime.WorkerRegistrar", InvalidUUIDRegistrar)

    with pytest.raises(ValueError):
        await create_worker("http://localhost:8000", registration, 10.0, object())


@pytest.mark.asyncio
async def test_create_worker_raises_key_error_for_missing_id_in_response(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    class MissingIdRegistrar(FakeRegistrar):
        def __init__(self, base_url: str) -> None:
            self.register = AsyncMock(return_value={})

    monkeypatch.setattr("forge.worker.runtime.WorkerRegistrar", MissingIdRegistrar)

    with pytest.raises(KeyError):
        await create_worker("http://localhost:8000", registration, 10.0, object())


@pytest.mark.asyncio
async def test_create_worker_raises_runtime_error_when_worker_id_is_none(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    patch_runtime_components(monkeypatch)

    async def _noop_register(self: Worker) -> None:
        self.id = None

    monkeypatch.setattr(Worker, "register", _noop_register)

    with pytest.raises(RuntimeError, match="Worker registration did not return a worker ID"):
        await create_worker("http://localhost:8000", registration, 10.0, object())


@pytest.mark.asyncio
async def test_create_worker_initializes_runtime_with_constructed_components(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    patch_runtime_components(monkeypatch)
    data_provider = object()

    worker = await create_worker("http://localhost:8000", registration, 10.0, data_provider)

    created_assignment_executor = RecordingAssignmentExecutor.instances[0]
    created_heartbeat_loop = RecordingHeartbeatLoop.instances[0]

    assert worker.assignment_executor is created_assignment_executor
    assert worker.heartbeat_loop is created_heartbeat_loop

    assert created_assignment_executor.details_client is RecordingIdClient.instances[0]
    assert created_assignment_executor.request_builder is RecordingRequestBuilder.instances[0]
    assert created_assignment_executor.assignment_handler is RecordingAssignmentHandler.instances[0]
    assert created_assignment_executor.lifecycle is RecordingLifecycle.instances[0]

    assert RecordingLifecycle.instances[0].executor is RecordingDockerExecutor.instances[0]
    assert RecordingLifecycle.instances[0].status_reporter is RecordingStatusReporter.instances[0]
