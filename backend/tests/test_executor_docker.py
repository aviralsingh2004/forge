from unittest.mock import Mock

import docker
import pytest
import requests
from docker.types import DeviceRequest

from forge.worker.executor.base import ExecutionRequest
from forge.worker.executor.docker import DockerExecutor


def test_docker_executor_initializes_client(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Mock()
    from_env = Mock(return_value=client)
    monkeypatch.setattr("forge.worker.executor.docker.docker.from_env", from_env)

    executor = DockerExecutor()

    from_env.assert_called_once_with()
    assert executor.client is client


def test_docker_executor_propagates_client_initialization_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = docker.errors.DockerException("Docker daemon unavailable")
    from_env = Mock(side_effect=error)
    monkeypatch.setattr("forge.worker.executor.docker.docker.from_env", from_env)

    with pytest.raises(docker.errors.DockerException, match="Docker daemon unavailable"):
        DockerExecutor()

    from_env.assert_called_once_with()


def _request(
    cpu_required: int = 2,
    memory_required_mb: int = 1024,
    gpu_required: int = 0,
    timeout_seconds: int | None = None,
) -> ExecutionRequest:
    return ExecutionRequest(
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        cpu_required=cpu_required,
        memory_required_mb=memory_required_mb,
        gpu_required=gpu_required,
        timeout_seconds=timeout_seconds,
    )


def _executor_with_container(
    monkeypatch: pytest.MonkeyPatch,
    container: Mock,
) -> tuple[DockerExecutor, Mock]:
    containers = Mock()
    containers.create.return_value = container
    client = Mock()
    client.containers = containers
    monkeypatch.setattr("forge.worker.executor.docker.docker.from_env", Mock(return_value=client))
    return DockerExecutor(), containers


@pytest.mark.asyncio
async def test_execute_creates_starts_waits_and_removes_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    result = await executor.execute(_request())

    containers.create.assert_called_once_with(
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        nano_cpus=2_000_000_000,
        mem_limit="1024m",
        device_requests=None,
    )
    container.start.assert_called_once_with()
    container.wait.assert_called_once_with()
    container.remove.assert_called_once_with()
    assert result.exit_code == 0


@pytest.mark.asyncio
async def test_execute_passes_cpu_limit_to_container_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(cpu_required=2, memory_required_mb=0))

    assert containers.create.call_args.kwargs["nano_cpus"] == 2_000_000_000


@pytest.mark.asyncio
async def test_execute_passes_memory_limit_to_container_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(cpu_required=0, memory_required_mb=1024))

    assert containers.create.call_args.kwargs["mem_limit"] == "1024m"


@pytest.mark.asyncio
async def test_execute_passes_combined_resource_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(cpu_required=2, memory_required_mb=1024))

    assert containers.create.call_args.kwargs == {
        "image": "python:3.12",
        "command": ["python", "-c", "print(1)"],
        "nano_cpus": 2_000_000_000,
        "mem_limit": "1024m",
        "device_requests": None,
    }


@pytest.mark.asyncio
async def test_execute_passes_zero_resource_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(cpu_required=0, memory_required_mb=0))

    assert containers.create.call_args.kwargs["nano_cpus"] == 0
    assert containers.create.call_args.kwargs["mem_limit"] == "0m"


@pytest.mark.asyncio
async def test_execute_passes_no_gpu_device_requests_when_gpu_not_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(gpu_required=0))

    assert containers.create.call_args.kwargs["device_requests"] is None


@pytest.mark.asyncio
async def test_execute_requests_one_gpu(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(gpu_required=1))

    device_requests = containers.create.call_args.kwargs["device_requests"]
    assert len(device_requests) == 1
    assert isinstance(device_requests[0], DeviceRequest)
    assert device_requests[0].count == 1
    assert device_requests[0].capabilities == [["gpu"]]


@pytest.mark.asyncio
async def test_execute_requests_multiple_gpus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(gpu_required=2))

    device_requests = containers.create.call_args.kwargs["device_requests"]
    assert len(device_requests) == 1
    assert device_requests[0].count == 2
    assert device_requests[0].capabilities == [["gpu"]]


@pytest.mark.asyncio
async def test_execute_preserves_cpu_memory_and_gpu_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, containers = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(cpu_required=2, memory_required_mb=1024, gpu_required=1))

    create_kwargs = containers.create.call_args.kwargs
    assert create_kwargs["image"] == "python:3.12"
    assert create_kwargs["command"] == ["python", "-c", "print(1)"]
    assert create_kwargs["nano_cpus"] == 2_000_000_000
    assert create_kwargs["mem_limit"] == "1024m"
    device_requests = create_kwargs["device_requests"]
    assert len(device_requests) == 1
    assert device_requests[0].count == 1
    assert device_requests[0].capabilities == [["gpu"]]


@pytest.mark.asyncio
async def test_execute_returns_nonzero_exit_code_and_removes_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 1}
    executor, _ = _executor_with_container(monkeypatch, container)

    result = await executor.execute(_request())

    assert result.exit_code == 1
    container.remove.assert_called_once_with()


@pytest.mark.asyncio
async def test_execute_does_not_request_logs_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, _ = _executor_with_container(monkeypatch, container)

    result = await executor.execute(_request())

    assert result.error_message is None
    container.logs.assert_not_called()


@pytest.mark.asyncio
async def test_execute_decodes_nonzero_container_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 1}
    container.logs.return_value = b"container failed\n"
    executor, _ = _executor_with_container(monkeypatch, container)

    result = await executor.execute(_request())

    container.logs.assert_called_once_with()
    assert result.exit_code == 1
    assert result.error_message == "container failed\n"


@pytest.mark.asyncio
async def test_execute_replaces_invalid_utf8_in_error_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 1}
    container.logs.return_value = b"bad-\xff-log"
    executor, _ = _executor_with_container(monkeypatch, container)

    result = await executor.execute(_request())

    assert result.error_message == "bad-\ufffd-log"


@pytest.mark.asyncio
async def test_execute_returns_no_error_message_for_empty_error_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 1}
    container.logs.return_value = b""
    executor, _ = _executor_with_container(monkeypatch, container)

    result = await executor.execute(_request())

    assert result.error_message is None


@pytest.mark.asyncio
async def test_execute_passes_timeout_to_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, _ = _executor_with_container(monkeypatch, container)

    await executor.execute(_request(timeout_seconds=30))

    container.wait.assert_called_once_with(timeout=30)


@pytest.mark.asyncio
async def test_execute_waits_without_timeout_when_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 0}
    executor, _ = _executor_with_container(monkeypatch, container)

    await executor.execute(_request())

    container.wait.assert_called_once_with()


@pytest.mark.asyncio
async def test_execute_handles_timeout_by_stopping_and_returning_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.side_effect = requests.exceptions.ReadTimeout("timed out")
    executor, _ = _executor_with_container(monkeypatch, container)

    result = await executor.execute(_request(timeout_seconds=12))

    container.stop.assert_called_once_with()
    container.remove.assert_called_once_with()
    assert result.exit_code == -1
    assert result.error_message == "Container execution timed out after 12 seconds"


@pytest.mark.asyncio
async def test_execute_propagates_container_creation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    containers = Mock()
    error = docker.errors.DockerException("create failed")
    containers.create.side_effect = error
    client = Mock(containers=containers)
    monkeypatch.setattr("forge.worker.executor.docker.docker.from_env", Mock(return_value=client))

    with pytest.raises(docker.errors.DockerException, match="create failed"):
        await DockerExecutor().execute(_request())

    containers.create.assert_called_once()


@pytest.mark.asyncio
async def test_execute_propagates_container_start_error_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.start.side_effect = docker.errors.DockerException("start failed")
    executor, _ = _executor_with_container(monkeypatch, container)

    with pytest.raises(docker.errors.DockerException, match="start failed"):
        await executor.execute(_request())

    container.remove.assert_called_once_with()


@pytest.mark.asyncio
async def test_execute_propagates_container_wait_error_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.side_effect = docker.errors.DockerException("wait failed")
    executor, _ = _executor_with_container(monkeypatch, container)

    with pytest.raises(docker.errors.DockerException, match="wait failed"):
        await executor.execute(_request())

    container.remove.assert_called_once_with()


@pytest.mark.asyncio
async def test_execute_propagates_log_error_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.return_value = {"StatusCode": 1}
    container.logs.side_effect = docker.errors.DockerException("logs failed")
    executor, _ = _executor_with_container(monkeypatch, container)

    with pytest.raises(docker.errors.DockerException, match="logs failed"):
        await executor.execute(_request())

    container.remove.assert_called_once_with()


@pytest.mark.asyncio
async def test_execute_removes_container_when_start_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.start.side_effect = RuntimeError("container start failed")
    executor, _ = _executor_with_container(monkeypatch, container)

    with pytest.raises(RuntimeError, match="container start failed"):
        await executor.execute(_request())

    container.remove.assert_called_once_with()
    container.wait.assert_not_called()


@pytest.mark.asyncio
async def test_execute_removes_container_when_wait_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = Mock()
    container.wait.side_effect = RuntimeError("container wait failed")
    executor, _ = _executor_with_container(monkeypatch, container)

    with pytest.raises(RuntimeError, match="container wait failed"):
        await executor.execute(_request())

    container.start.assert_called_once_with()
    container.remove.assert_called_once_with()
