from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from forge.db.models import (
    Assignment,
    AssignmentStatus,
    Attempt,
    AttemptStatus,
    Job,
    JobStatus,
    Reservation,
    ReservationStatus,
    Worker,
    WorkerStatus,
)


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
            .with_for_update()
        )
        return result.scalar_one_or_none()

    async def get_ready_workers(self) -> list[Worker]:
        result = await self.session.execute(
            select(Worker)
            .where(Worker.status == WorkerStatus.READY)
            .options(selectinload(Worker.reservations))
        )
        return list(result.scalars().all())

    def filter_workers(
        self,
        job: Job,
        workers: list[Worker],
    ) -> list[Worker]:
        eligible_workers = []

        for worker in workers:
            available_cpu, available_memory_mb, available_gpu = self.get_available_resources(worker)

            if (
                available_cpu < job.cpu_required
                or available_memory_mb < job.memory_required_mb
                or available_gpu < job.gpu_required
            ):
                continue

            if not set(job.required_capabilities).issubset(worker.capabilities):
                continue

            eligible_workers.append(worker)

        return eligible_workers

    def score_worker(self, job: Job, worker: Worker) -> tuple[int, int, int]:
        available_cpu, available_memory_mb, available_gpu = self.get_available_resources(worker)

        return (
            available_cpu - job.cpu_required,
            available_memory_mb - job.memory_required_mb,
            available_gpu - job.gpu_required,
        )

    def select_worker(
        self,
        job: Job,
        workers: list[Worker],
    ) -> Worker | None:
        if not workers:
            return None

        return min(
            workers,
            key=lambda worker: self.score_worker(job, worker),
        )

    def get_available_resources(
        self,
        worker: Worker,
    ) -> tuple[int, int, int]:
        reserved_cpu = 0
        reserved_memory_mb = 0
        reserved_gpu = 0

        for reservation in worker.reservations:
            if reservation.status == ReservationStatus.ACTIVE:
                reserved_cpu += reservation.cpu_reserved
                reserved_memory_mb += reservation.memory_reserved_mb
                reserved_gpu += reservation.gpu_reserved

        return (
            worker.cpu_capacity - reserved_cpu,
            worker.memory_capacity_mb - reserved_memory_mb,
            worker.gpu_capacity - reserved_gpu,
        )

    async def create_attempt(self, job: Job, worker: Worker) -> Attempt:
        attempt = Attempt(
            job_id=job.id,
            worker_id=worker.id,
            attempt_number=1,
            status=AttemptStatus.CREATED,
        )

        self.session.add(attempt)
        await self.session.flush()

        return attempt

    async def create_reservation(
        self,
        job: Job,
        worker: Worker,
        attempt: Attempt,
    ) -> Reservation:
        reservation = Reservation(
            worker_id=worker.id,
            attempt_id=attempt.id,
            cpu_reserved=job.cpu_required,
            memory_reserved_mb=job.memory_required_mb,
            gpu_reserved=job.gpu_required,
            status=ReservationStatus.ACTIVE,
        )

        self.session.add(reservation)
        await self.session.flush()

        return reservation

    async def create_assignment(
        self,
        attempt: Attempt,
        worker: Worker,
    ) -> Assignment:
        assignment = Assignment(
            attempt_id=attempt.id,
            worker_id=worker.id,
            status=AssignmentStatus.CREATED,
        )

        self.session.add(assignment)
        await self.session.flush()

        return assignment

    def mark_job_assigned(self, job: Job) -> None:
        job.status = JobStatus.ASSIGNED

    async def lock_worker(self, worker: Worker) -> Worker:
        result = await self.session.execute(
            select(Worker)
            .where(Worker.id == worker.id)
            .options(selectinload(Worker.reservations))
            .with_for_update()
        )
        return result.scalar_one()

    def validate_worker_resources(self, job: Job, worker: Worker) -> bool:
        available_cpu, available_memory_mb, available_gpu = self.get_available_resources(worker)

        return (
            available_cpu >= job.cpu_required
            and available_memory_mb >= job.memory_required_mb
            and available_gpu >= job.gpu_required
            and set(job.required_capabilities).issubset(worker.capabilities)
        )

    async def schedule_job(self, job: Job, worker: Worker) -> Assignment:
        locked_job = await self.session.get(
            Job,
            job.id,
            with_for_update=True,
        )

        if locked_job is None:
            raise ValueError("Job not found")

        if locked_job.status != JobStatus.QUEUED:
            raise ValueError("Job is no longer queued")

        locked_worker = await self.lock_worker(worker)

        if not self.validate_worker_resources(locked_job, locked_worker):
            raise ValueError("Worker no longer has sufficient resources")

        attempt = await self.create_attempt(locked_job, locked_worker)
        await self.create_reservation(locked_job, locked_worker, attempt)
        assignment = await self.create_assignment(attempt, locked_worker)
        self.mark_job_assigned(locked_job)

        await self.session.commit()

        return assignment

    async def schedule_next_job(self) -> Assignment | None:
        job = await self.get_next_job()

        if job is None:
            return None

        workers = await self.get_ready_workers()
        eligible_workers = self.filter_workers(job, workers)
        worker = self.select_worker(job, eligible_workers)

        if worker is None:
            return None

        return await self.schedule_job(job, worker)
