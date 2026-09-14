from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from forge.db.models import Job, JobStatus


class Scheduler:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_next_job(self) -> Job | None:
        result = await self.session.execute(
            select(Job)
            .where(Job.status == JobStatus.QUEUED)
            .order_by(
                Job.priority.desc(),
                Job.created_at.asc(),
            )
            .limit(1)
        )
        return result.scalar_one_or_none()
