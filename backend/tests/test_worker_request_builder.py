from uuid import UUID

from forge.worker.assignment_details import AssignmentDetails
from forge.worker.request_builder import ExecutionRequestBuilder


def test_request_builder_maps_assignment_details_to_new_execution_request() -> None:
    details = AssignmentDetails(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000002"),
        worker_id=UUID("00000000-0000-0000-0000-000000000003"),
        image="python:3.12",
        command=["python", "script.py"],
        cpu_required=2,
        memory_required_mb=1024,
        gpu_required=1,
        timeout_seconds=30,
    )

    result = ExecutionRequestBuilder().build(details)

    assert result.image == details.image
    assert result.command == details.command
    assert result.cpu_required == details.cpu_required
    assert result.memory_required_mb == details.memory_required_mb
    assert result.gpu_required == details.gpu_required
    assert result.timeout_seconds == details.timeout_seconds
    assert result is not details
