from unittest.mock import AsyncMock

import httpx
import pytest

from forge.worker.registration import WorkerRegistrar, WorkerRegistration


class FakeResponse:
    def __init__(self, payload: dict[str, object], request: httpx.Request) -> None:
        self.payload = payload
        self.request = request
        self.raise_for_status = Mock()

    def json(self) -> dict[str, object]:
        return self.payload


class Mock:
    def __init__(self) -> None:
        self.called = False

    def __call__(self) -> None:
        self.called = True


class AsyncClientContext:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.post = AsyncMock(return_value=response)
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> "AsyncClientContext":
        self.entered = True
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exited = True


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


def response_for(payload: dict[str, object]) -> FakeResponse:
    request = httpx.Request("POST", "http://test/api/v1/workers/register")
    return FakeResponse(payload, request)


@pytest.mark.asyncio
async def test_register_posts_registration_and_returns_response(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    payload = {"worker_id": "worker-id", "status": "REGISTERING"}
    client = AsyncClientContext(response_for(payload))
    monkeypatch.setattr("forge.worker.registration.httpx.AsyncClient", lambda: client)

    result = await WorkerRegistrar("http://forge.test").register(registration)

    assert result == payload
    client.post.assert_awaited_once_with(
        "http://forge.test/api/v1/workers/register",
        json={
            "name": "worker-1",
            "version": "0.1.0",
            "cpu_capacity": 8,
            "memory_capacity_mb": 16384,
            "gpu_capacity": 1,
            "capabilities": ["docker", "python"],
        },
    )


@pytest.mark.asyncio
async def test_register_normalizes_base_url_trailing_slash(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    client = AsyncClientContext(response_for({"worker_id": "worker-id"}))
    monkeypatch.setattr("forge.worker.registration.httpx.AsyncClient", lambda: client)

    await WorkerRegistrar("http://forge.test/").register(registration)

    assert client.post.await_args.args[0] == "http://forge.test/api/v1/workers/register"


@pytest.mark.asyncio
async def test_register_raises_for_http_error(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    request = httpx.Request("POST", "http://forge.test/api/v1/workers/register")
    response = httpx.Response(503, request=request)
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.registration.httpx.AsyncClient", lambda: client)

    with pytest.raises(httpx.HTTPStatusError):
        await WorkerRegistrar("http://forge.test").register(registration)


@pytest.mark.asyncio
async def test_register_enters_and_exits_async_client(
    monkeypatch: pytest.MonkeyPatch,
    registration: WorkerRegistration,
) -> None:
    client = AsyncClientContext(response_for({"worker_id": "worker-id"}))
    monkeypatch.setattr("forge.worker.registration.httpx.AsyncClient", lambda: client)

    await WorkerRegistrar("http://forge.test").register(registration)

    assert client.entered is True
    assert client.exited is True
