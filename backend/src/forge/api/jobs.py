from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from forge.api.schemas.jobs import JobCreate, JobResponse, JobUpdate
from forge.db.models import Job, JobEvent
from forge.db.session import get_session
from forge.messaging.redis_streams import SchedulingEvent
from forge.outbox.service import add_outbox_event, scheduling_event_payload

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


@router.post("", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    job_data: JobCreate,
    session: AsyncSession = Depends(get_session),
) -> JobResponse:
    job = Job(
        id=uuid4(),
        name=job_data.name,
        image=job_data.image,
        command=job_data.command,
        priority=job_data.priority,
        cpu_required=job_data.cpu_required,
        memory_required_mb=job_data.memory_required_mb,
        gpu_required=job_data.gpu_required,
        required_capabilities=job_data.required_capabilities,
        max_retries=job_data.max_retries,
    )
    session.add(job)

    await session.flush()

    event = SchedulingEvent(
        event_id=uuid4(),
        event_type="JOB_CREATED",
        job_id=job.id,
        created_at=job.created_at,
    )

    add_outbox_event(
        session,
        event_type=event.event_type,
        payload=scheduling_event_payload(event),
    )

    await session.commit()

    return job


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> JobResponse:
    result = await session.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    return job


@router.patch("/{job_id}", response_model=JobResponse)
async def update_job(
    job_id: UUID,
    job_data: JobUpdate,
    session: AsyncSession = Depends(get_session),
) -> JobResponse:
    result = await session.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    updates = job_data.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update",
        )

    for field, value in updates.items():
        setattr(job, field, value)

    job_event = JobEvent(
        job_id=job.id,
        event_type="JOB_UPDATED",
        event_metadata={
            "updated_fields": list(updates.keys()),
        },
    )

    session.add(job_event)

    event = SchedulingEvent(
        event_id=uuid4(),
        event_type="JOB_UPDATED",
        job_id=job.id,
        created_at=job.created_at,
    )
    add_outbox_event(
        session,
        event_type=event.event_type,
        payload=scheduling_event_payload(event),
    )
    try:
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    await session.refresh(job)

    return job


@router.get("", response_model=list[JobResponse])
async def list_jobs(
    session: AsyncSession = Depends(get_session),
) -> list[JobResponse]:
    result = await session.execute(
        select(Job).order_by(
            Job.priority.desc(),
            Job.created_at.asc(),
        )
    )
    return list(result.scalars().all())
