from uuid import UUID

import httpx


class AssignmentHandler:
    def __init__(self, base_url: str, worker_id: UUID) -> None:
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id

    async def acknowledge(self, assignment_id: UUID) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/v1/workers/"
                f"{self.worker_id}/assignments/{assignment_id}/acknowledge"
            )
            response.raise_for_status()
