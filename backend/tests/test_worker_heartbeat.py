import asyncio
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest

from forge.worker.heartbeat import HeartbeatLoop, WorkerHeartbeat


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


def _worker_heartbeat() -> WorkerHeartbeat:
    return WorkerHeartbeat(
        "http://forge.test/",
        UUID("00000000-0000-0000-0000-000000000001"),
    )


@pytest.mark.asyncio
async def test_send_posts_heartbeat_payload_and_checks_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = FakeResponse()
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.heartbeat.httpx.AsyncClient", lambda: client)
    attempt_ids = [
        UUID("00000000-0000-0000-0000-000000000011"),
        UUID("00000000-0000-0000-0000-000000000012"),
    ]

    await _worker_heartbeat().send(3, 2048, 1, attempt_ids)

    client.post.assert_awaited_once_with(
        "http://forge.test/api/v1/workers/00000000-0000-0000-0000-000000000001/heartbeat",
        json={
            "cpu_usage": 3,
            "memory_usage_mb": 2048,
            "gpu_usage": 1,
            "running_attempts": [str(attempt_id) for attempt_id in attempt_ids],
        },
    )
    response.raise_for_status.assert_called_once_with()


@pytest.mark.asyncio
async def test_send_posts_empty_running_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = FakeResponse()
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.heartbeat.httpx.AsyncClient", lambda: client)

    await _worker_heartbeat().send(1, 256, 0, [])

    assert client.post.await_args.kwargs["json"]["running_attempts"] == []


@pytest.mark.asyncio
async def test_send_propagates_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = httpx.Request(
        "POST",
        "http://forge.test/api/v1/workers/00000000-0000-0000-0000-000000000001/heartbeat",
    )
    response = httpx.Response(503, request=request)
    client = AsyncClientContext(response)
    monkeypatch.setattr("forge.worker.heartbeat.httpx.AsyncClient", lambda: client)

    with pytest.raises(httpx.HTTPStatusError):
        await _worker_heartbeat().send(1, 256, 0, [])


@pytest.mark.asyncio
async def test_send_enters_and_exits_async_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClientContext(FakeResponse())
    monkeypatch.setattr("forge.worker.heartbeat.httpx.AsyncClient", lambda: client)

    await _worker_heartbeat().send(1, 256, 0, [])

    assert client.entered is True
    assert client.exited is True


@pytest.mark.asyncio
async def test_loop_sends_data_from_provider() -> None:
    heartbeat = AsyncMock()
    data_provider = AsyncMock(
        return_value=(3, 2048, 1, [UUID("00000000-0000-0000-0000-000000000011")])
    )
    loop = HeartbeatLoop(heartbeat, 10.0, data_provider)

    async def _send_side_effect(*args: object, **kwargs: object) -> None:
        loop.stop()

    heartbeat.send.side_effect = _send_side_effect

    await loop.run()

    data_provider.assert_awaited_once_with()
    heartbeat.send.assert_awaited_once_with(
        cpu_usage=3,
        memory_usage_mb=2048,
        gpu_usage=1,
        running_attempts=[UUID("00000000-0000-0000-0000-000000000011")],
    )


@pytest.mark.asyncio
async def test_loop_repeats_and_sleeps_at_configured_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    heartbeat = AsyncMock()
    data_provider = AsyncMock(
        return_value=(1, 256, 0, [UUID("00000000-0000-0000-0000-000000000012")])
    )
    interval = 0.005
    loop = HeartbeatLoop(heartbeat, interval, data_provider)

    recorded_timeouts: list[float | None] = []
    real_wait_for = asyncio.wait_for

    async def _spy_wait_for(fut: object, timeout: float | None = None) -> object:
        recorded_timeouts.append(timeout)
        return await real_wait_for(fut, timeout=timeout)  # type: ignore[arg-type]

    monkeypatch.setattr("forge.worker.heartbeat.asyncio.wait_for", _spy_wait_for)

    call_count = 0

    async def _send_side_effect(*args: object, **kwargs: object) -> None:
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            loop.stop()

    heartbeat.send.side_effect = _send_side_effect

    await loop.run()

    assert data_provider.await_count == 2
    assert heartbeat.send.await_count == 2
    heartbeat.send.assert_any_await(
        cpu_usage=1,
        memory_usage_mb=256,
        gpu_usage=0,
        running_attempts=[UUID("00000000-0000-0000-0000-000000000012")],
    )
    assert loop.interval_seconds == interval
    assert recorded_timeouts == [interval, interval]


@pytest.mark.asyncio
async def test_loop_stop_terminates_while_waiting() -> None:
    heartbeat = AsyncMock()
    data_provider = AsyncMock(
        return_value=(1, 256, 0, [UUID("00000000-0000-0000-0000-000000000012")])
    )
    loop = HeartbeatLoop(heartbeat, 100.0, data_provider)

    task = asyncio.create_task(loop.run())

    while heartbeat.send.await_count == 0:
        await asyncio.sleep(0.001)

    loop.stop()

    await asyncio.wait_for(task, timeout=1.0)

    assert heartbeat.send.await_count == 1
    assert task.done()


@pytest.mark.asyncio
async def test_loop_propagates_data_provider_failure() -> None:
    heartbeat = AsyncMock()
    error = RuntimeError("usage collection failed")
    data_provider = AsyncMock(side_effect=error)
    loop = HeartbeatLoop(heartbeat, 10.0, data_provider)

    with pytest.raises(RuntimeError, match="usage collection failed"):
        await loop.run()

    heartbeat.send.assert_not_awaited()
