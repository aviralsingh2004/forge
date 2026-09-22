from uuid import UUID

from forge.worker.assignment_execution import AssignmentExecutor
from forge.worker.executor.base import ExecutionResult
from forge.worker.heartbeat import HeartbeatLoop
from forge.worker.registration import WorkerRegistrar, WorkerRegistration


class Worker:
    def __init__(
        self,
        registration: WorkerRegistration,
        registrar: WorkerRegistrar,
    ) -> None:
        self.registration = registration
        self.registrar = registrar
        self.assignment_executor: AssignmentExecutor | None = None
        self.heartbeat_loop: HeartbeatLoop | None = None
        self.id: UUID | None = None

    async def register(self) -> None:
        response = await self.registrar.register(self.registration)
        self.id = UUID(response["id"])

    def initialize_runtime(
        self,
        assignment_executor: AssignmentExecutor,
        heartbeat_loop: HeartbeatLoop,
    ) -> None:
        if self.id is None:
            raise RuntimeError("Worker must be registered before runtime initialization")

        self.assignment_executor = assignment_executor
        self.heartbeat_loop = heartbeat_loop

    async def execute_assignment(self, assignment_id: UUID) -> ExecutionResult:
        if self.assignment_executor is None:
            raise RuntimeError("Worker runtime is not initialized")

        return await self.assignment_executor.execute(assignment_id)

    async def run_heartbeat(self) -> None:
        if self.heartbeat_loop is None:
            raise RuntimeError("Worker runtime is not initialized")

        await self.heartbeat_loop.run()

    def stop(self) -> None:
        if self.heartbeat_loop is not None:
            self.heartbeat_loop.stop()
