from dataclasses import FrozenInstanceError

import pytest

from forge.worker.executor.base import ExecutionRequest, ExecutionResult, Executor


def test_execution_request_stores_fields_and_is_frozen() -> None:
    request = ExecutionRequest(
        image="python:3.12",
        command=["python", "script.py"],
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=1,
    )

    assert request.image == "python:3.12"
    assert request.command == ["python", "script.py"]
    assert request.cpu_required == 2
    assert request.memory_required_mb == 1024
    assert request.gpu_required == 1
    assert request.timeout_seconds is None

    with pytest.raises(FrozenInstanceError):
        request.image = "alpine:latest"

    with pytest.raises(FrozenInstanceError):
        request.timeout_seconds = 30


def test_execution_result_stores_fields_and_defaults_error_message() -> None:
    result = ExecutionResult(exit_code=0)
    assert result.exit_code == 0
    assert result.error_message is None

    failed_result = ExecutionResult(exit_code=1, error_message="container failed")
    assert failed_result.exit_code == 1
    assert failed_result.error_message == "container failed"

    with pytest.raises(FrozenInstanceError):
        failed_result.exit_code = 2


def test_executor_is_abstract() -> None:
    with pytest.raises(TypeError):
        Executor()


@pytest.mark.asyncio
async def test_concrete_executor_returns_execution_result() -> None:
    class TestExecutor(Executor):
        async def execute(self, request: ExecutionRequest) -> ExecutionResult:
            return ExecutionResult(exit_code=0)

    request = ExecutionRequest(
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        cpu_required=1,
        memory_required_mb=256,
        gpu_required=0,
    )

    result = await TestExecutor().execute(request)

    assert result == ExecutionResult(exit_code=0)
