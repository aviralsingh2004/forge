from forge.worker.assignment import AssignmentHandler
from forge.worker.assignment_details import AssignmentDetailsClient
from forge.worker.assignment_execution import AssignmentExecutor
from forge.worker.executor.docker import DockerExecutor
from forge.worker.heartbeat import HeartbeatLoop, WorkerHeartbeat
from forge.worker.lifecycle import WorkerExecutionLifecycle
from forge.worker.registration import WorkerRegistrar, WorkerRegistration
from forge.worker.request_builder import ExecutionRequestBuilder
from forge.worker.status import AttemptStatusReporter
from forge.worker.worker import Worker


async def create_worker(
    base_url: str,
    registration: WorkerRegistration,
    heartbeat_interval_seconds: float,
    data_provider,
) -> Worker:
    registrar = WorkerRegistrar(base_url)

    worker = Worker(
        registration=registration,
        registrar=registrar,
    )

    await worker.register()

    if worker.id is None:
        raise RuntimeError("Worker registration did not return a worker ID")

    assignment_details_client = AssignmentDetailsClient(
        base_url=base_url,
        worker_id=worker.id,
    )

    assignment_handler = AssignmentHandler(
        base_url=base_url,
        worker_id=worker.id,
    )

    heartbeat = WorkerHeartbeat(
        base_url=base_url,
        worker_id=worker.id,
    )

    docker_executor = DockerExecutor()

    status_reporter = AttemptStatusReporter(
        base_url=base_url,
    )

    lifecycle = WorkerExecutionLifecycle(
        executor=docker_executor,
        status_reporter=status_reporter,
    )

    request_builder = ExecutionRequestBuilder()

    assignment_executor = AssignmentExecutor(
        details_client=assignment_details_client,
        request_builder=request_builder,
        assignment_handler=assignment_handler,
        lifecycle=lifecycle,
    )

    heartbeat_loop = HeartbeatLoop(
        heartbeat=heartbeat,
        interval_seconds=heartbeat_interval_seconds,
        data_provider=data_provider,
    )

    worker.initialize_runtime(
        assignment_executor=assignment_executor,
        heartbeat_loop=heartbeat_loop,
    )

    return worker
