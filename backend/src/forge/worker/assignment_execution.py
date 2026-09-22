from uuid import UUID

from forge.worker.assignment import AssignmentHandler
from forge.worker.assignment_details import AssignmentDetailsClient
from forge.worker.executor.base import ExecutionResult
from forge.worker.lifecycle import WorkerExecutionLifecycle
from forge.worker.request_builder import ExecutionRequestBuilder


class AssignmentExecutor:
    def __init__(
        self,
        details_client: AssignmentDetailsClient,
        request_builder: ExecutionRequestBuilder,
        assignment_handler: AssignmentHandler,
        lifecycle: WorkerExecutionLifecycle,
    ) -> None:
        self.details_client = details_client
        self.request_builder = request_builder
        self.assignment_handler = assignment_handler
        self.lifecycle = lifecycle

    async def execute(self, assignment_id: UUID) -> ExecutionResult:
        details = await self.details_client.get(assignment_id)

        request = self.request_builder.build(details)

        await self.assignment_handler.acknowledge(assignment_id)

        return await self.lifecycle.execute(
            attempt_id=details.attempt_id,
            request=request,
        )
