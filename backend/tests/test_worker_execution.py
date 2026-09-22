from unittest.mock import AsyncMock, Mock

import pytest

from forge.worker.execution import WorkerExecution
from forge.worker.executor.base import ExecutionRequest, ExecutionResult, Executor


def _request() -> ExecutionRequest:
    return ExecutionRequest(
        image="python:3.12",
        command=["python", "-c", "print(1)"],
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=0,
    )


@pytest.mark.asyncio
async def test_execute_delegates_request_and_returns_executor_result() -> None:
    request = _request()
    result = ExecutionResult(exit_code=0)
    executor = Mock(spec=Executor)
    executor.execute = AsyncMock(return_value=result)
    worker_execution = WorkerExecution(executor)

    returned_result = await worker_execution.execute(request)

    executor.execute.assert_awaited_once_with(request)
    assert returned_result is result


@pytest.mark.asyncio
async def test_execute_returns_failed_execution_result_unchanged() -> None:
    request = _request()
    result = ExecutionResult(exit_code=1, error_message="container failed")
    executor = Mock(spec=Executor)
    executor.execute = AsyncMock(return_value=result)
    worker_execution = WorkerExecution(executor)

    returned_result = await worker_execution.execute(request)

    executor.execute.assert_awaited_once_with(request)
    assert returned_result is result
    assert returned_result.exit_code == 1
    assert returned_result.error_message == "container failed"


@pytest.mark.asyncio
async def test_execute_propagates_executor_exception() -> None:
    request = _request()
    error = RuntimeError("executor failed")
    executor = Mock(spec=Executor)
    executor.execute = AsyncMock(side_effect=error)
    worker_execution = WorkerExecution(executor)

    with pytest.raises(RuntimeError, match="executor failed"):
        await worker_execution.execute(request)

    executor.execute.assert_awaited_once_with(request)
