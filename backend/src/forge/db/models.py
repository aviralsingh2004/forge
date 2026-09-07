import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    ARRAY,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from forge.db.base import Base


class StrEnum(str, enum.Enum):
    @classmethod
    def values(cls) -> list[str]:
        return [item.value for item in cls]


class WorkerStatus(StrEnum):
    REGISTERING = "REGISTERING"
    READY = "READY"
    BUSY = "BUSY"
    DRAINING = "DRAINING"
    OFFLINE = "OFFLINE"


class JobStatus(StrEnum):
    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AttemptStatus(StrEnum):
    CREATED = "CREATED"
    ASSIGNED = "ASSIGNED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AssignmentStatus(StrEnum):
    CREATED = "CREATED"
    DELIVERED = "DELIVERED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ReservationStatus(StrEnum):
    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"


def enum_type(enum_class: type[StrEnum], name: str) -> Enum:
    return Enum(
        *enum_class.values(),
        name=name,
        native_enum=True,
        create_constraint=False,
    )


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class Worker(TimestampMixin, Base):
    __tablename__ = "workers"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    status: Mapped[WorkerStatus] = mapped_column(
        enum_type(WorkerStatus, "worker_status"), nullable=False, default=WorkerStatus.REGISTERING
    )
    version: Mapped[str] = mapped_column(String(100), nullable=False)
    cpu_capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    memory_capacity_mb: Mapped[int] = mapped_column(Integer, nullable=False)
    gpu_capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    capabilities: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    attempts: Mapped[list["Attempt"]] = relationship(back_populates="worker")
    assignments: Mapped[list["Assignment"]] = relationship(back_populates="worker")
    reservations: Mapped[list["Reservation"]] = relationship(back_populates="worker")
    heartbeats: Mapped[list["WorkerHeartbeat"]] = relationship(back_populates="worker")

    __table_args__ = (
        CheckConstraint("cpu_capacity >= 0", name="ck_workers_cpu_capacity_nonnegative"),
        CheckConstraint("memory_capacity_mb >= 0", name="ck_workers_memory_capacity_nonnegative"),
        CheckConstraint("gpu_capacity >= 0", name="ck_workers_gpu_capacity_nonnegative"),
        Index("ix_workers_status", "status"),
        Index("ix_workers_last_heartbeat_at", "last_heartbeat_at"),
    )


class Job(TimestampMixin, Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    image: Mapped[str] = mapped_column(String(255), nullable=False)
    command: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cpu_required: Mapped[int] = mapped_column(Integer, nullable=False)
    memory_required_mb: Mapped[int] = mapped_column(Integer, nullable=False)
    gpu_required: Mapped[int] = mapped_column(Integer, nullable=False)
    required_capabilities: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    status: Mapped[JobStatus] = mapped_column(
        enum_type(JobStatus, "job_status"), nullable=False, default=JobStatus.QUEUED
    )
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    attempts: Mapped[list["Attempt"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )
    events: Mapped[list["JobEvent"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint("priority >= 0 AND priority <= 100", name="ck_jobs_priority_range"),
        CheckConstraint("cpu_required >= 0", name="ck_jobs_cpu_required_nonnegative"),
        CheckConstraint("memory_required_mb >= 0", name="ck_jobs_memory_required_nonnegative"),
        CheckConstraint("gpu_required >= 0", name="ck_jobs_gpu_required_nonnegative"),
        CheckConstraint("max_retries >= 0", name="ck_jobs_max_retries_nonnegative"),
        Index(
            "ix_jobs_scheduling_order",
            "status",
            text("priority DESC"),
            text("created_at ASC"),
        ),
    )


class Attempt(TimestampMixin, Base):
    __tablename__ = "attempts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workers.id", ondelete="SET NULL")
    )
    status: Mapped[AttemptStatus] = mapped_column(
        enum_type(AttemptStatus, "attempt_status"), nullable=False, default=AttemptStatus.CREATED
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exit_code: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)

    job: Mapped[Job] = relationship(back_populates="attempts")
    worker: Mapped[Worker | None] = relationship(back_populates="attempts")
    assignment: Mapped["Assignment | None"] = relationship(back_populates="attempt", uselist=False)
    reservations: Mapped[list["Reservation"]] = relationship(back_populates="attempt")
    events: Mapped[list["JobEvent"]] = relationship(back_populates="attempt")

    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_attempts_job_attempt_number"),
        CheckConstraint("attempt_number > 0", name="ck_attempts_attempt_number_positive"),
        Index("ix_attempts_job_id", "job_id"),
        Index("ix_attempts_worker_id", "worker_id"),
    )


class Assignment(Base):
    __tablename__ = "assignments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    attempt_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("attempts.id", ondelete="CASCADE"), nullable=False
    )
    worker_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workers.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[AssignmentStatus] = mapped_column(
        enum_type(AssignmentStatus, "assignment_status"),
        nullable=False,
        default=AssignmentStatus.CREATED,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    attempt: Mapped[Attempt] = relationship(back_populates="assignment")
    worker: Mapped[Worker] = relationship(back_populates="assignments")

    __table_args__ = (UniqueConstraint("attempt_id", name="uq_assignments_attempt_id"),)


class Reservation(Base):
    __tablename__ = "reservations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    worker_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workers.id", ondelete="RESTRICT"), nullable=False
    )
    attempt_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("attempts.id", ondelete="CASCADE"), nullable=False
    )
    cpu_reserved: Mapped[int] = mapped_column(Integer, nullable=False)
    memory_reserved_mb: Mapped[int] = mapped_column(Integer, nullable=False)
    gpu_reserved: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ReservationStatus] = mapped_column(
        enum_type(ReservationStatus, "reservation_status"),
        nullable=False,
        default=ReservationStatus.ACTIVE,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    worker: Mapped[Worker] = relationship(back_populates="reservations")
    attempt: Mapped[Attempt] = relationship(back_populates="reservation")

    __table_args__ = (
        CheckConstraint("cpu_reserved >= 0", name="ck_reservations_cpu_nonnegative"),
        CheckConstraint("memory_reserved_mb >= 0", name="ck_reservations_memory_nonnegative"),
        CheckConstraint("gpu_reserved >= 0", name="ck_reservations_gpu_nonnegative"),
        Index(
            "ix_reservations_active_worker",
            "worker_id",
            postgresql_where=text("status = 'ACTIVE'"),
        ),
        Index(
            "uq_reservations_active_attempt",
            "attempt_id",
            unique=True,
            postgresql_where=text("status = 'ACTIVE'"),
        ),
    )


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    worker_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workers.id", ondelete="CASCADE"), nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    cpu_usage: Mapped[int] = mapped_column(Integer, nullable=False)
    memory_usage_mb: Mapped[int] = mapped_column(Integer, nullable=False)
    gpu_usage: Mapped[int] = mapped_column(Integer, nullable=False)
    running_attempts: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)

    worker: Mapped[Worker] = relationship(back_populates="heartbeats")

    __table_args__ = (
        CheckConstraint("cpu_usage >= 0", name="ck_heartbeats_cpu_usage_nonnegative"),
        CheckConstraint("memory_usage_mb >= 0", name="ck_heartbeats_memory_usage_nonnegative"),
        CheckConstraint("gpu_usage >= 0", name="ck_heartbeats_gpu_usage_nonnegative"),
        Index(
            "ix_worker_heartbeats_worker_timestamp",
            "worker_id",
            text("timestamp DESC"),
        ),
    )


class JobEvent(Base):
    __tablename__ = "job_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    attempt_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("attempts.id", ondelete="SET NULL")
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    event_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    job: Mapped[Job] = relationship(back_populates="events")
    attempt: Mapped[Attempt | None] = relationship(back_populates="events")

    __table_args__ = (
        Index("ix_job_events_job_created", "job_id", text("created_at DESC")),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("retry_count >= 0", name="ck_outbox_retry_count_nonnegative"),
        Index(
            "ix_outbox_unpublished_created",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )
