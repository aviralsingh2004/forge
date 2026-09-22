from forge.worker.executor.base import (
    ExecutionRequest,
    ExecutionResult,
    Executor,
)


class WorkerExecution:
    def __init__(self, executor: Executor) -> None:
        self.executor = executor

    async def execute(
        self,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        return await self.executor.execute(request)
