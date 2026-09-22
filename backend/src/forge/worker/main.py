import asyncio
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

import uvicorn
from fastapi import FastAPI

from forge.worker.heartbeat import HeartbeatDataProvider
from forge.worker.registration import WorkerRegistration
from forge.worker.runtime import create_worker
from forge.worker.server import create_worker_app


@dataclass(frozen=True)
class WorkerConfig:
    forge_api_url: str = "http://localhost:8000"
    name: str = "forge-worker"
    version: str = "0.1.0"
    cpu_capacity: int = 4
    memory_capacity_mb: int = 8192
    gpu_capacity: int = 0
    capabilities: tuple[str, ...] = ("docker", "python")
    heartbeat_interval_seconds: float = 10.0
    host: str = "0.0.0.0"
    port: int = 8001

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        raw_capabilities = os.getenv("WORKER_CAPABILITIES")
        if raw_capabilities is not None:
            capabilities = tuple(c.strip() for c in raw_capabilities.split(",") if c.strip())
        else:
            capabilities = ("docker", "python")

        return cls(
            forge_api_url=os.getenv("FORGE_API_URL", "http://localhost:8000"),
            name=os.getenv("WORKER_NAME", "forge-worker"),
            version=os.getenv("WORKER_VERSION", "0.1.0"),
            cpu_capacity=int(os.getenv("WORKER_CPU_CAPACITY", "4")),
            memory_capacity_mb=int(os.getenv("WORKER_MEMORY_CAPACITY_MB", "8192")),
            gpu_capacity=int(os.getenv("WORKER_GPU_CAPACITY", "0")),
            capabilities=capabilities,
            heartbeat_interval_seconds=float(
                os.getenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "10.0")
            ),
            host=os.getenv("WORKER_HOST", "0.0.0.0"),
            port=int(os.getenv("WORKER_PORT", "8001")),
        )

    def to_registration(self) -> WorkerRegistration:
        return WorkerRegistration(
            name=self.name,
            version=self.version,
            cpu_capacity=self.cpu_capacity,
            memory_capacity_mb=self.memory_capacity_mb,
            gpu_capacity=self.gpu_capacity,
            capabilities=list(self.capabilities),
        )


async def default_data_provider() -> tuple[int, int, int, list[UUID]]:
    return (0, 0, 0, [])


async def serve_worker_app(
    app: FastAPI,
    host: str,
    port: int,
) -> None:
    config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def run_worker(
    config: WorkerConfig | None = None,
    server_runner: Callable[..., Awaitable[None]] = serve_worker_app,
    data_provider: HeartbeatDataProvider = default_data_provider,
) -> None:
    if config is None:
        config = WorkerConfig.from_env()

    registration = config.to_registration()
    worker = await create_worker(
        base_url=config.forge_api_url,
        registration=registration,
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
        data_provider=data_provider,
    )

    app = create_worker_app(worker)

    heartbeat_task = asyncio.create_task(worker.run_heartbeat())
    server_task = asyncio.create_task(server_runner(app, config.host, config.port))

    try:
        done, _ = await asyncio.wait(
            [heartbeat_task, server_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            task.result()
    finally:
        worker.stop()
        heartbeat_task.cancel()
        server_task.cancel()
        await asyncio.gather(heartbeat_task, server_task, return_exceptions=True)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
