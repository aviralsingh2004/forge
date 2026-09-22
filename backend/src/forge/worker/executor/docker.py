import asyncio

import docker
import requests
from docker.types import DeviceRequest

from forge.worker.executor.base import ExecutionRequest, ExecutionResult


class DockerExecutor:
    def __init__(self) -> None:
        self.client = docker.from_env()

    def _wait_for_container(
        self,
        container,
        timeout_seconds: int | None,
    ) -> dict:
        if timeout_seconds is None:
            return container.wait()

        return container.wait(timeout=timeout_seconds)

    def _stop_container(self, container) -> None:
        container.stop()

    async def _remove_container(self, container) -> None:
        await asyncio.to_thread(container.remove)

    async def execute(
        self,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        device_requests = None
        if request.gpu_required > 0:
            device_requests = [
                DeviceRequest(
                    count=request.gpu_required,
                    capabilities=[["gpu"]],
                )
            ]
        container = await asyncio.to_thread(
            self.client.containers.create,
            image=request.image,
            command=request.command,
            nano_cpus=request.cpu_required * 1_000_000_000,
            mem_limit=f"{request.memory_required_mb}m",
            device_requests=device_requests,
        )

        try:
            await asyncio.to_thread(container.start)

            result = await asyncio.to_thread(
                self._wait_for_container,
                container,
                request.timeout_seconds,
            )

            error_message = None

            if result["StatusCode"] != 0:
                logs = await asyncio.to_thread(container.logs)
                error_message = logs.decode("utf-8", errors="replace") if logs else None

            return ExecutionResult(
                exit_code=result["StatusCode"],
                error_message=error_message,
            )
        except requests.exceptions.ReadTimeout:
            await asyncio.to_thread(self._stop_container, container)

            return ExecutionResult(
                exit_code=-1,
                error_message=f"Container execution timed out after"
                f" {request.timeout_seconds} seconds",
            )
        finally:
            await self._remove_container(container)
