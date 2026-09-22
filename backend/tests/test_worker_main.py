import asyncio
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from fastapi import FastAPI

from forge.worker.main import (
    WorkerConfig,
    default_data_provider,
    run_worker,
)
from forge.worker.registration import WorkerRegistration


def test_worker_config_defaults() -> None:
    config = WorkerConfig()

    assert config.forge_api_url == "http://localhost:8000"
    assert config.name == "forge-worker"
    assert config.version == "0.1.0"
    assert config.cpu_capacity == 4
    assert config.memory_capacity_mb == 8192
    assert config.gpu_capacity == 0
    assert config.capabilities == ("docker", "python")
    assert config.heartbeat_interval_seconds == 10.0
    assert config.host == "0.0.0.0"
    assert config.port == 8001


def test_worker_config_from_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in (
        "FORGE_API_URL",
        "WORKER_NAME",
        "WORKER_VERSION",
        "WORKER_CPU_CAPACITY",
        "WORKER_MEMORY_CAPACITY_MB",
        "WORKER_GPU_CAPACITY",
        "WORKER_CAPABILITIES",
        "WORKER_HEARTBEAT_INTERVAL_SECONDS",
        "WORKER_HOST",
        "WORKER_PORT",
    ):
        monkeypatch.delenv(env_var, raising=False)

    config = WorkerConfig.from_env()

    assert config.forge_api_url == "http://localhost:8000"
    assert config.name == "forge-worker"
    assert config.version == "0.1.0"
    assert config.cpu_capacity == 4
    assert config.memory_capacity_mb == 8192
    assert config.gpu_capacity == 0
    assert config.capabilities == ("docker", "python")
    assert config.heartbeat_interval_seconds == 10.0
    assert config.host == "0.0.0.0"
    assert config.port == 8001


def test_worker_config_from_env_custom(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FORGE_API_URL", "http://forge-api.internal:9000")
    monkeypatch.setenv("WORKER_NAME", "worker-node-42")
    monkeypatch.setenv("WORKER_VERSION", "1.2.3")
    monkeypatch.setenv("WORKER_CPU_CAPACITY", "16")
    monkeypatch.setenv("WORKER_MEMORY_CAPACITY_MB", "32768")
    monkeypatch.setenv("WORKER_GPU_CAPACITY", "2")
    monkeypatch.setenv("WORKER_CAPABILITIES", "docker, cuda, custom-exec")
    monkeypatch.setenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "5.5")
    monkeypatch.setenv("WORKER_HOST", "127.0.0.1")
    monkeypatch.setenv("WORKER_PORT", "9001")

    config = WorkerConfig.from_env()

    assert config.forge_api_url == "http://forge-api.internal:9000"
    assert config.name == "worker-node-42"
    assert config.version == "1.2.3"
    assert config.cpu_capacity == 16
    assert isinstance(config.cpu_capacity, int)
    assert config.memory_capacity_mb == 32768
    assert isinstance(config.memory_capacity_mb, int)
    assert config.gpu_capacity == 2
    assert isinstance(config.gpu_capacity, int)
    assert config.capabilities == ("docker", "cuda", "custom-exec")
    assert config.heartbeat_interval_seconds == 5.5
    assert isinstance(config.heartbeat_interval_seconds, float)
    assert config.host == "127.0.0.1"
    assert config.port == 9001
    assert isinstance(config.port, int)


def test_worker_config_from_env_capability_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_CAPABILITIES", " docker , , python ,  , gpu , ")

    config = WorkerConfig.from_env()

    assert config.capabilities == ("docker", "python", "gpu")


def test_worker_config_to_registration() -> None:
    config = WorkerConfig(
        forge_api_url="http://localhost:8000",
        name="test-worker",
        version="0.2.0",
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=1,
        capabilities=("docker", "gpu"),
        heartbeat_interval_seconds=15.0,
    )

    registration = config.to_registration()

    assert isinstance(registration, WorkerRegistration)
    assert registration.name == "test-worker"
    assert registration.version == "0.2.0"
    assert registration.cpu_capacity == 8
    assert registration.memory_capacity_mb == 16384
    assert registration.gpu_capacity == 1
    assert registration.capabilities == ["docker", "gpu"]
    assert isinstance(registration.capabilities, list)


@pytest.mark.asyncio
async def test_default_data_provider_returns_expected_tuple() -> None:
    result = await default_data_provider()

    assert isinstance(result, tuple)
    assert len(result) == 4
    cpu, memory, gpu, attempts = result
    assert cpu == 0
    assert memory == 0
    assert gpu == 0
    assert attempts == []


@pytest.mark.asyncio
async def test_run_worker_lifecycle_and_wiring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_worker = Mock()
    mock_worker.id = UUID("00000000-0000-0000-0000-000000000001")
    mock_worker.run_heartbeat = AsyncMock()
    mock_worker.stop = Mock()

    mock_create_worker = AsyncMock(return_value=mock_worker)
    monkeypatch.setattr("forge.worker.main.create_worker", mock_create_worker)

    mock_app = FastAPI()
    mock_create_app = Mock(return_value=mock_app)
    monkeypatch.setattr("forge.worker.main.create_worker_app", mock_create_app)

    server_started = asyncio.Event()
    heartbeat_started = asyncio.Event()

    async def _mock_run_heartbeat() -> None:
        heartbeat_started.set()
        await asyncio.sleep(10)

    mock_worker.run_heartbeat.side_effect = _mock_run_heartbeat

    async def _mock_server_runner(app: object, host: str, port: int) -> None:
        assert app is mock_app
        assert host == "127.0.0.1"
        assert port == 8001
        server_started.set()
        # Sleep briefly then complete, which should trigger run_worker completion & cleanup
        await asyncio.sleep(0.01)

    config = WorkerConfig(
        forge_api_url="http://forge.test:8000",
        name="worker-test",
        version="0.1.0",
        cpu_capacity=4,
        memory_capacity_mb=4096,
        gpu_capacity=0,
        capabilities=("docker",),
        heartbeat_interval_seconds=7.5,
        host="127.0.0.1",
        port=8001,
    )

    data_provider = AsyncMock(return_value=(0, 0, 0, []))

    await run_worker(
        config=config,
        server_runner=_mock_server_runner,
        data_provider=data_provider,
    )

    mock_create_worker.assert_awaited_once_with(
        base_url="http://forge.test:8000",
        registration=config.to_registration(),
        heartbeat_interval_seconds=7.5,
        data_provider=data_provider,
    )
    mock_create_app.assert_called_once_with(mock_worker)
    assert server_started.is_set()
    assert heartbeat_started.is_set()
    mock_worker.stop.assert_called_once()


@pytest.mark.asyncio
async def test_run_worker_propagates_heartbeat_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_worker = Mock()
    mock_worker.run_heartbeat = AsyncMock(side_effect=RuntimeError("Heartbeat crashed"))
    mock_worker.stop = Mock()

    mock_create_worker = AsyncMock(return_value=mock_worker)
    monkeypatch.setattr("forge.worker.main.create_worker", mock_create_worker)

    async def _mock_server_runner(app: object, host: str, port: int) -> None:
        await asyncio.sleep(10)

    config = WorkerConfig()

    with pytest.raises(RuntimeError, match="Heartbeat crashed"):
        await run_worker(
            config=config,
            server_runner=_mock_server_runner,
        )

    mock_worker.stop.assert_called_once()


@pytest.mark.asyncio
async def test_run_worker_propagates_server_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_worker = Mock()

    async def _mock_run_heartbeat() -> None:
        await asyncio.sleep(10)

    mock_worker.run_heartbeat = AsyncMock(side_effect=_mock_run_heartbeat)
    mock_worker.stop = Mock()

    mock_create_worker = AsyncMock(return_value=mock_worker)
    monkeypatch.setattr("forge.worker.main.create_worker", mock_create_worker)

    async def _mock_server_runner(app: object, host: str, port: int) -> None:
        raise RuntimeError("Server failed to bind")

    config = WorkerConfig()

    with pytest.raises(RuntimeError, match="Server failed to bind"):
        await run_worker(
            config=config,
            server_runner=_mock_server_runner,
        )

    mock_worker.stop.assert_called_once()
