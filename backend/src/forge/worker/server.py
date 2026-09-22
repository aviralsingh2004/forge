from uuid import UUID

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from forge.worker.worker import Worker


class AssignmentDeliveryRequest(BaseModel):
    assignment_id: UUID


def create_worker_router(worker: Worker) -> APIRouter:
    router = APIRouter()

    @router.post("/assignments")
    async def deliver_assignment(
        assignment: AssignmentDeliveryRequest,
    ) -> dict[str, str]:
        await worker.execute_assignment(assignment.assignment_id)

        return {"status": "accepted"}

    return router


def create_worker_app(worker: Worker) -> FastAPI:
    app = FastAPI()
    app.include_router(create_worker_router(worker))
    return app
