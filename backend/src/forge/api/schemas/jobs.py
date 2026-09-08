from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class JobCreate(BaseModel):
    name: str
    image: str
    command: list[str]

    priority: int = Field(default=0, ge=0, le=100)

    cpu_required: int = Field(ge=0)
    memory_required_mb: int = Field(ge=0)
    gpu_required: int = Field(ge=0)

    required_capabilities: list[str] = Field(default_factory=list)

    max_retries: int = Field(default=0, ge=0)


class JobUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1)
    image: str | None = Field(default=None, min_length=1)
    command: list[str] | None = None
    priority: int | None = Field(default=None, ge=0, le=100)
    cpu_required: int | None = Field(default=None, ge=0)
    memory_required_mb: int | None = Field(default=None, ge=0)
    gpu_required: int | None = Field(default=None, ge=0)
    required_capabilities: list[str] | None = None
    max_retries: int | None = Field(default=None, ge=0)


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    image: str
    command: list[str]
    priority: int
    cpu_required: int
    memory_required_mb: int
    gpu_required: int
    required_capabilities: list[str]
    status: str
    max_retries: int
    created_at: datetime
    updated_at: datetime
