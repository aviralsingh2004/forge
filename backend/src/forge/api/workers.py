from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from forge.api.schemas.workers import (
    AssignmentRequest,
    AssignmentResponse,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerRegister,
    WorkerResponse,
)
from forge.db.models import Assignment, AssignmentStatus, Worker, WorkerHeartbeat
from forge.db.session import get_session

router = APIRouter(prefix="/api/v1/workers", tags=["workers"])


@router.post("/register", response_model=WorkerResponse, status_code=status.HTTP_201_CREATED)
async def register_worker(
    worker_data: WorkerRegister, session: AsyncSession = Depends(get_session)
) -> WorkerResponse:
    worker = Worker(
        id=uuid4(),
        name=worker_data.name,
        version=worker_data.version,
        cpu_capacity=worker_data.cpu_capacity,
        memory_capacity_mb=worker_data.memory_capacity_mb,
        gpu_capacity=worker_data.gpu_capacity,
        capabilities=worker_data.capabilities,
    )

    session.add(worker)

    await session.commit()
    await session.refresh(worker)

    return worker


@router.post("/{worker_id}/heartbeat", response_model=WorkerHeartbeatResponse)
async def worker_heartbeat(
    worker_id: UUID,
    heartbeat_data: WorkerHeartbeatRequest,
    session: AsyncSession = Depends(get_session),
) -> WorkerHeartbeatResponse:
    result = await session.execute(select(Worker).where(Worker.id == worker_id))
    worker = result.scalar_one_or_none()

    if worker is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Worker not found",
        )
    heartbeat_time = datetime.now(UTC)
    worker.last_heartbeat_at = heartbeat_time
    heartbeat = WorkerHeartbeat(
        worker_id=worker.id,
        timestamp=heartbeat_time,
        cpu_usage=heartbeat_data.cpu_usage,
        memory_usage_mb=heartbeat_data.memory_usage_mb,
        gpu_usage=heartbeat_data.gpu_usage,
        running_attempts=[str(attempt_id) for attempt_id in heartbeat_data.running_attempts],
    )

    session.add(heartbeat)

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    return WorkerHeartbeatResponse(
        worker_id=worker.id,
        timestamp=heartbeat_time,
    )


@router.post("/{worker_id}/assignments", response_model=AssignmentResponse)
async def deliver_assignment(
    worker_id: UUID,
    assignment_data: AssignmentRequest,
    session: AsyncSession = Depends(get_session),
) -> AssignmentResponse:
    result = await session.execute(select(Worker).where(Worker.id == worker_id))
    worker = result.scalar_one_or_none()

    if worker is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Worker not found",
        )
    result = await session.execute(
        select(Assignment).where(Assignment.id == assignment_data.assignment_id)
    )
    assignment = result.scalar_one_or_none()
    if assignment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assignment not found",
        )
    if assignment.worker_id != worker.id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Assignment does not belong to worker",
        )
    if assignment.status != AssignmentStatus.CREATED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Assignment is not in CREATED state",
        )
    assignment.status = AssignmentStatus.DELIVERED
    assignment.delivered_at = datetime.now(UTC)

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    await session.refresh(assignment)

    return assignment
