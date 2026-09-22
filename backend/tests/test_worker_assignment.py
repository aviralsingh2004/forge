from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest

from forge.worker.assignment import AssignmentHandler


class FakeResponse:
    def __init__(self) -> None:
        self.raise_for_status = Mock()


class AsyncClientContext:
    def __init__(self, response: FakeResponse) -> None:
        self.post = AsyncMock(return_value=response)
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> "AsyncClientContext":
        self.entered = True
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exited = True


def _handler(base_url: str = "http://forge.test") -> AssignmentHandler:
    return AssignmentHandler(
        base_url,
        UUID("00000000-0000-0000-0000-000000000001"),
    )


@pytest.mark.asyncio
async def test_acknowledge_posts_expected_url_and_checks_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = FakeResponse()
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.assignment.httpx.AsyncClient", lambda: client)
    assignment_id = UUID("00000000-0000-0000-0000-000000000011")

    await _handler().acknowledge(assignment_id)

    client.post.assert_awaited_once_with(
        "http://forge.test/api/v1/workers/00000000-0000-0000-0000-000000000001/"
        "assignments/00000000-0000-0000-0000-000000000011/acknowledge"
    )
    response.raise_for_status.assert_called_once_with()


@pytest.mark.asyncio
async def test_acknowledge_normalizes_base_url_trailing_slash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClientContext(FakeResponse())
    monkeypatch.setattr("forge.worker.assignment.httpx.AsyncClient", lambda: client)
    assignment_id = UUID("00000000-0000-0000-0000-000000000011")

    await _handler("http://forge.test/").acknowledge(assignment_id)

    assert client.post.await_args.args[0] == (
        "http://forge.test/api/v1/workers/00000000-0000-0000-0000-000000000001/"
        "assignments/00000000-0000-0000-0000-000000000011/acknowledge"
    )


@pytest.mark.asyncio
async def test_acknowledge_propagates_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = httpx.Request(
        "POST",
        "http://forge.test/api/v1/workers/00000000-0000-0000-0000-000000000001/"
        "assignments/00000000-0000-0000-0000-000000000011/acknowledge",
    )
    response = httpx.Response(409, request=request)
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.assignment.httpx.AsyncClient", lambda: client)

    with pytest.raises(httpx.HTTPStatusError):
        await _handler().acknowledge(UUID("00000000-0000-0000-0000-000000000011"))


@pytest.mark.asyncio
async def test_acknowledge_enters_and_exits_async_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClientContext(FakeResponse())
    monkeypatch.setattr("forge.worker.assignment.httpx.AsyncClient", lambda: client)

    await _handler().acknowledge(UUID("00000000-0000-0000-0000-000000000011"))

    assert client.entered is True
    assert client.exited is True
