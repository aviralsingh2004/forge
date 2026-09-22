from dataclasses import dataclass
from uuid import UUID

import httpx


@dataclass(frozen=True)
class AssignmentDetails:
    id: UUID
    attempt_id: UUID
    worker_id: UUID
    image: str
    command: list[str]
    cpu_required: int
    memory_required_mb: int
    gpu_required: int
    timeout_seconds: int | None


class AssignmentDetailsClient:
    def __init__(self, base_url: str, worker_id: UUID) -> None:
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id

    async def get(self, assignment_id: UUID) -> AssignmentDetails:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/v1/workers/{self.worker_id}/assignments/{assignment_id}"
            )
            response.raise_for_status()

            data = response.json()

            return AssignmentDetails(
                id=UUID(data["id"]),
                attempt_id=UUID(data["attempt_id"]),
                worker_id=UUID(data["worker_id"]),
                image=data["image"],
                command=data["command"],
                cpu_required=data["cpu_required"],
                memory_required_mb=data["memory_required_mb"],
                gpu_required=data["gpu_required"],
                timeout_seconds=data["timeout_seconds"],
            )
