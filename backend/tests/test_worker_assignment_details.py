from dataclasses import FrozenInstanceError
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest

from forge.worker.assignment_details import AssignmentDetails, AssignmentDetailsClient

DETAILS_ID = UUID("00000000-0000-0000-0000-000000000001")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000002")
WORKER_ID = UUID("00000000-0000-0000-0000-000000000003")
ASSIGNMENT_ID = UUID("00000000-0000-0000-0000-000000000004")


def _details() -> AssignmentDetails:
    return AssignmentDetails(
        id=DETAILS_ID,
        attempt_id=ATTEMPT_ID,
        worker_id=WORKER_ID,
        image="python:3.12",
        command=["python", "script.py"],
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=1,
        timeout_seconds=None,
    )


class AsyncClientContext:
    def __init__(self, response: Mock) -> None:
        self.get = AsyncMock(return_value=response)
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> "AsyncClientContext":
        self.entered = True
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exited = True


def _response() -> Mock:
    response = Mock()
    response.json.return_value = {
        "id": str(DETAILS_ID),
        "attempt_id": str(ATTEMPT_ID),
        "worker_id": str(WORKER_ID),
        "image": "python:3.12",
        "command": ["python", "script.py"],
        "cpu_required": 2,
        "memory_required_mb": 1024,
        "gpu_required": 1,
        "timeout_seconds": None,
    }
    return response


def test_assignment_details_preserves_fields_and_is_frozen() -> None:
    details = _details()

    assert details.id == DETAILS_ID
    assert details.attempt_id == ATTEMPT_ID
    assert details.worker_id == WORKER_ID
    assert details.image == "python:3.12"
    assert details.command == ["python", "script.py"]
    assert details.cpu_required == 2
    assert details.memory_required_mb == 1024
    assert details.gpu_required == 1
    assert details.timeout_seconds is None

    with pytest.raises(FrozenInstanceError):
        details.image = "alpine:latest"


@pytest.mark.asyncio
async def test_details_client_gets_and_maps_assignment_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _response()
    response.raise_for_status = Mock()
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.assignment_details.httpx.AsyncClient", lambda: client)

    result = await AssignmentDetailsClient("http://forge.test", WORKER_ID).get(ASSIGNMENT_ID)

    client.get.assert_awaited_once_with(
        "http://forge.test/api/v1/workers/00000000-0000-0000-0000-000000000003/"
        "assignments/00000000-0000-0000-0000-000000000004"
    )
    response.raise_for_status.assert_called_once_with()
    assert result == _details()
    assert isinstance(result.id, UUID)
    assert result.timeout_seconds is None
    assert client.entered is True
    assert client.exited is True


@pytest.mark.asyncio
async def test_details_client_normalizes_base_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _response()
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.assignment_details.httpx.AsyncClient", lambda: client)

    await AssignmentDetailsClient("http://forge.test/", WORKER_ID).get(ASSIGNMENT_ID)

    assert client.get.await_args.args[0] == (
        "http://forge.test/api/v1/workers/00000000-0000-0000-0000-000000000003/"
        "assignments/00000000-0000-0000-0000-000000000004"
    )


@pytest.mark.asyncio
async def test_details_client_propagates_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = httpx.Request("GET", "http://forge.test/assignment")
    response = httpx.Response(404, request=request)
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.assignment_details.httpx.AsyncClient", lambda: client)

    with pytest.raises(httpx.HTTPStatusError):
        await AssignmentDetailsClient("http://forge.test", WORKER_ID).get(ASSIGNMENT_ID)
