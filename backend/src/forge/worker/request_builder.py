from forge.worker.assignment_details import AssignmentDetails
from forge.worker.executor.base import ExecutionRequest


class ExecutionRequestBuilder:
    def build(self, details: AssignmentDetails) -> ExecutionRequest:
        return ExecutionRequest(
            image=details.image,
            command=details.command,
            cpu_required=details.cpu_required,
            memory_required_mb=details.memory_required_mb,
            gpu_required=details.gpu_required,
            timeout_seconds=details.timeout_seconds,
        )
