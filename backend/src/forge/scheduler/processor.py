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
        job_id_str = data.get("job_id")

        if event_type != "JOB_CREATED":
            return

        if job_id_str is None:
            raise ValueError("JOB_CREATED event is missing job_id")

        job_id = UUID(job_id_str)

        # Phase 9 R3: idempotency — look up the specific job referenced by this event.
        # schedule_event_job() returns:
        #   Assignment  — job was QUEUED; scheduling completed normally.
        #   None        — job is no longer QUEUED (already handled); safe to ACK.
        # Raises ValueError if the job does not exist.
        # Raises RuntimeError if the job is QUEUED but no eligible worker exists.
        assignment = await self.scheduler.schedule_event_job(job_id)

        if assignment is None:
            # Job was already scheduled (or otherwise left the QUEUED state).
            # Treat the event as obsolete — the runner will ACK it safely.
            return

        # Scheduling completed: assignment created, caller will ACK.
