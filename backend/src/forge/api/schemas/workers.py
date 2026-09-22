from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from forge.db.models import AssignmentStatus, AttemptStatus, WorkerStatus


class WorkerRegister(BaseModel):
    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    cpu_capacity: int = Field(ge=0)
    memory_capacity_mb: int = Field(ge=0)
    gpu_capacity: int = Field(ge=0)
    capabilities: list[str] = Field(default_factory=list)


class WorkerResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    status: WorkerStatus
    version: str
    cpu_capacity: int
    memory_capacity_mb: int
    gpu_capacity: int
    capabilities: list[str]
    last_heartbeat_at: datetime | None
    created_at: datetime
    updated_at: datetime


class WorkerHeartbeatRequest(BaseModel):
    cpu_usage: int = Field(ge=0)
    memory_usage_mb: int = Field(ge=0)
    gpu_usage: int = Field(ge=0)
    running_attempts: list[UUID] = Field(default_factory=list)


class WorkerHeartbeatResponse(BaseModel):
    worker_id: UUID
    timestamp: datetime


class AssignmentRequest(BaseModel):
    assignment_id: UUID


class AssignmentResponse(BaseModel):
    id: UUID
    attempt_id: UUID
    worker_id: UUID
    status: AssignmentStatus
    created_at: datetime
    delivered_at: datetime | None
    acknowledged_at: datetime | None
    completed_at: datetime | None


class AssignmentDetailsResponse(BaseModel):
    id: UUID
    attempt_id: UUID
    worker_id: UUID
    status: AssignmentStatus
    image: str
    command: list[str]
    cpu_required: int
    memory_required_mb: int
    gpu_required: int
    timeout_seconds: int | None


class AttemptStatusRequest(BaseModel):
    status: AttemptStatus
    exit_code: int | None = None
    error_message: str | None = None


class AttemptStatusResponse(BaseModel):
    id: UUID
    status: AttemptStatus
    started_at: datetime | None
    finished_at: datetime | None
    exit_code: int | None
    error_message: str | None
