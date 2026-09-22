import asyncio
from collections.abc import Awaitable, Callable
from uuid import UUID

import httpx

HeartbeatDataProvider = Callable[
    [],
    Awaitable[tuple[int, int, int, list[UUID]]],
]


class WorkerHeartbeat:
    def __init__(self, base_url: str, worker_id: UUID) -> None:
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id

    async def send(
        self,
        cpu_usage: int,
        memory_usage_mb: int,
        gpu_usage: int,
        running_attempts: list[UUID],
    ) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/v1/workers/{self.worker_id}/heartbeat",
                json={
                    "cpu_usage": cpu_usage,
                    "memory_usage_mb": memory_usage_mb,
                    "gpu_usage": gpu_usage,
                    "running_attempts": [str(attempt_id) for attempt_id in running_attempts],
                },
            )
            response.raise_for_status()


class HeartbeatLoop:
    def __init__(
        self,
        heartbeat: WorkerHeartbeat,
        interval_seconds: float,
        data_provider: HeartbeatDataProvider,
    ) -> None:
        self.heartbeat = heartbeat
        self.interval_seconds = interval_seconds
        self.data_provider = data_provider
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            cpu_usage, memory_usage_mb, gpu_usage, running_attempts = await self.data_provider()

            await self.heartbeat.send(
                cpu_usage=cpu_usage,
                memory_usage_mb=memory_usage_mb,
                gpu_usage=gpu_usage,
                running_attempts=running_attempts,
            )

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.interval_seconds,
                )
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop_event.set()
