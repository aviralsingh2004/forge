from uuid import UUID

from forge.scheduler.service import Scheduler


class SchedulerEventProcessor:
    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    async def process(
        self,
        message_id: str,
        data: dict[str, str],
    ) -> None:
        event_type = data.get("event_type")
        job_id = data.get("job_id")

        if event_type != "JOB_CREATED":
            return

        if job_id is None:
            raise ValueError("JOB_CREATED event is missing job_id")

        assignment = await self.scheduler.schedule_next_job()

        if assignment is None:
            raise RuntimeError(f"Unable to schedule JOB_CREATED event for job {UUID(job_id)}")
