from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from forge.api.schemas.workers import AttemptStatusRequest, AttemptStatusResponse
from forge.db.models import Attempt, AttemptStatus
from forge.db.session import get_session

router = APIRouter(prefix="/api/v1/attempts", tags=["attempts"])

ALLOWED_ATTEMPT_TRANSITIONS = {
    AttemptStatus.ASSIGNED: {AttemptStatus.STARTING},
    AttemptStatus.STARTING: {AttemptStatus.RUNNING},
    AttemptStatus.RUNNING: {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
    },
}


@router.post("/{attempt_id}/status", response_model=AttemptStatusResponse)
async def update_attempt_status(
    attempt_id: UUID,
    status_data: AttemptStatusRequest,
    session: AsyncSession = Depends(get_session),
) -> AttemptStatusResponse:
    result = await session.execute(select(Attempt).where(Attempt.id == attempt_id))
    attempt = result.scalar_one_or_none()

    if attempt is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Attempt not found",
        )

    allowed_statuses = ALLOWED_ATTEMPT_TRANSITIONS.get(attempt.status, set())

    if status_data.status not in allowed_statuses:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Invalid attempt status transition: {attempt.status} -> {status_data.status}",
        )

    attempt.status = status_data.status

    if status_data.status == AttemptStatus.STARTING:
        if attempt.started_at is None:
            attempt.started_at = datetime.now(UTC)

    elif status_data.status in {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
    }:
        attempt.finished_at = datetime.now(UTC)

    if status_data.exit_code is not None:
        attempt.exit_code = status_data.exit_code

    if status_data.error_message is not None:
        attempt.error_message = status_data.error_message

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    await session.refresh(attempt)

    return attempt
