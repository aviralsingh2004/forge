from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class WorkerRegistration:
    name: str
    version: str
    cpu_capacity: int
    memory_capacity_mb: int
    gpu_capacity: int
    capabilities: list[str]


class WorkerRegistrar:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def register(self, registration: WorkerRegistration) -> dict:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/v1/workers/register",
                json={
                    "name": registration.name,
                    "version": registration.version,
                    "cpu_capacity": registration.cpu_capacity,
                    "memory_capacity_mb": registration.memory_capacity_mb,
                    "gpu_capacity": registration.gpu_capacity,
                    "capabilities": registration.capabilities,
                },
            )
            response.raise_for_status()
            return response.json()
