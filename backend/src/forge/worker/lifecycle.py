from uuid import UUID

from forge.db.models import AttemptStatus
from forge.worker.executor.base import ExecutionRequest, ExecutionResult, Executor
from forge.worker.status import AttemptStatusReporter


class WorkerExecutionLifecycle:
    def __init__(
        self,
        executor: Executor,
        status_reporter: AttemptStatusReporter,
    ) -> None:
        self.executor = executor
        self.status_reporter = status_reporter

    async def execute(
        self,
        attempt_id: UUID,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        await self.status_reporter.report(
            attempt_id=attempt_id,
            status=AttemptStatus.STARTING,
        )

        result = await self.executor.execute(request)

        if result.exit_code == 0:
            status = AttemptStatus.SUCCEEDED
        else:
            status = AttemptStatus.FAILED

        await self.status_reporter.report(
            attempt_id=attempt_id,
            status=status,
            exit_code=result.exit_code,
            error_message=result.error_message,
        )

        return result
