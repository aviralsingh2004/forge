from uuid import UUID

import httpx

from forge.db.models import AttemptStatus


class AttemptStatusReporter:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def report(
        self,
        attempt_id: UUID,
        status: AttemptStatus,
        exit_code: int | None = None,
        error_message: str | None = None,
    ) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/v1/attempts/{attempt_id}/status",
                json={
                    "status": status,
                    "exit_code": exit_code,
                    "error_message": error_message,
                },
            )
            response.raise_for_status()
