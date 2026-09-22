from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionRequest:
    image: str
    command: list[str]
    cpu_required: int
    memory_required_mb: int
    gpu_required: int
    timeout_seconds: int | None = None


@dataclass(frozen=True)
class ExecutionResult:
    exit_code: int
    error_message: str | None = None


class Executor(ABC):
    @abstractmethod
    async def execute(
        self,
        request: ExecutionRequest,
    ) -> ExecutionResult: ...
