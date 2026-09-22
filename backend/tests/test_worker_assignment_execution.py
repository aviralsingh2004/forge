from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from forge.worker.assignment_details import AssignmentDetails
from forge.worker.assignment_execution import AssignmentExecutor
from forge.worker.executor.base import ExecutionRequest, ExecutionResult

ASSIGNMENT_ID = UUID("00000000-0000-0000-0000-000000000001")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000002")


def _details() -> AssignmentDetails:
    return AssignmentDetails(
        id=ASSIGNMENT_ID,
        attempt_id=ATTEMPT_ID,
        worker_id=UUID("00000000-0000-0000-0000-000000000003"),
        image="python:3.12",
        command=["python", "script.py"],
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=1,
        timeout_seconds=None,
    )


def _request() -> ExecutionRequest:
    return ExecutionRequest(
        image="python:3.12",
        command=["python", "script.py"],
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=1,
        timeout_seconds=None,
    )


def _executor() -> tuple[Mock, Mock, Mock, Mock]:
    details_client = Mock()
    request_builder = Mock()
    assignment_handler = Mock()
    lifecycle = Mock()
    details_client.get = AsyncMock()
    request_builder.build = Mock()
    assignment_handler.acknowledge = AsyncMock()
    lifecycle.execute = AsyncMock()
    return details_client, request_builder, assignment_handler, lifecycle


def _assignment_executor(
    details_client: Mock,
    request_builder: Mock,
    assignment_handler: Mock,
    lifecycle: Mock,
) -> AssignmentExecutor:
    return AssignmentExecutor(
        details_client,
        request_builder,
        assignment_handler,
        lifecycle,
    )


@pytest.mark.asyncio
async def test_execute_fetches_builds_acknowledges_and_runs_lifecycle() -> None:
    details = _details()
    request = _request()
    result = ExecutionResult(exit_code=0)
    details_client, request_builder, assignment_handler, lifecycle = _executor()
    details_client.get.return_value = details
    request_builder.build.return_value = request
    lifecycle.execute.return_value = result

    returned = await _assignment_executor(
        details_client, request_builder, assignment_handler, lifecycle
    ).execute(ASSIGNMENT_ID)

    details_client.get.assert_awaited_once_with(ASSIGNMENT_ID)
    request_builder.build.assert_called_once_with(details)
    assignment_handler.acknowledge.assert_awaited_once_with(ASSIGNMENT_ID)
    lifecycle.execute.assert_awaited_once_with(attempt_id=ATTEMPT_ID, request=request)
    assert returned is result


@pytest.mark.asyncio
async def test_details_failure_prevents_acknowledgement_and_execution() -> None:
    error = RuntimeError("details failed")
    details_client, request_builder, assignment_handler, lifecycle = _executor()
    details_client.get.side_effect = error

    with pytest.raises(RuntimeError, match="details failed"):
        await _assignment_executor(
            details_client, request_builder, assignment_handler, lifecycle
        ).execute(ASSIGNMENT_ID)

    request_builder.build.assert_not_called()
    assignment_handler.acknowledge.assert_not_awaited()
    lifecycle.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_request_build_failure_prevents_acknowledgement_and_execution() -> None:
    error = RuntimeError("request build failed")
    details_client, request_builder, assignment_handler, lifecycle = _executor()
    details_client.get.return_value = _details()
    request_builder.build.side_effect = error

    with pytest.raises(RuntimeError, match="request build failed"):
        await _assignment_executor(
            details_client, request_builder, assignment_handler, lifecycle
        ).execute(ASSIGNMENT_ID)

    assignment_handler.acknowledge.assert_not_awaited()
    lifecycle.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_acknowledgement_failure_prevents_execution() -> None:
    error = RuntimeError("acknowledgement failed")
    details_client, request_builder, assignment_handler, lifecycle = _executor()
    details_client.get.return_value = _details()
    request_builder.build.return_value = _request()
    assignment_handler.acknowledge.side_effect = error

    with pytest.raises(RuntimeError, match="acknowledgement failed"):
        await _assignment_executor(
            details_client, request_builder, assignment_handler, lifecycle
        ).execute(ASSIGNMENT_ID)

    lifecycle.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_lifecycle_exception_propagates() -> None:
    error = RuntimeError("lifecycle failed")
    details_client, request_builder, assignment_handler, lifecycle = _executor()
    details_client.get.return_value = _details()
    request_builder.build.return_value = _request()
    lifecycle.execute.side_effect = error

    with pytest.raises(RuntimeError, match="lifecycle failed"):
        await _assignment_executor(
            details_client, request_builder, assignment_handler, lifecycle
        ).execute(ASSIGNMENT_ID)
