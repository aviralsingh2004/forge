# Worker Architecture

## Purpose

The Worker is Forge's distributed execution engine component. Each Worker instance represents a discrete node capable of executing isolated, containerized workloads via Docker while reporting lifecycle and resource metrics back to the Forge Control Plane.

In Forge v0.1, Workers operate on a **push-based** model:
- Workers register their advertised resource capacities and capabilities with the Control Plane.
- Workers send periodic heartbeats to maintain visibility and health.
- The Control Plane / Scheduler delivers work to the Worker's dedicated HTTP server via `POST /assignments`.
- Workers fetch assignment details, execute the workload inside Docker containers enforcing CPU and memory constraints, and report attempt status transitions back to the Control Plane.

```text
               Forge Control Plane (FastAPI)
                    http://localhost:8000
                    ▲         │          ▲
                    │         │          │
   1. Register      │         │ 4. Push  │ 6. Status
   2. Heartbeat     │         │ Assignment Updates
   3. Fetch Details │         │          │
                    │         ▼          │
             ┌──────┴────────────────────┴──────┐
             │              Worker              │
             │                                  │
             │  - Heartbeat Loop                │
             │  - Worker HTTP Server (:8001)    │
             │  - Assignment Executor           │
             │  - Status Reporter               │
             └────────────────┬─────────────────┘
                              │
                              │ 5. Container Run
                              ▼
                         Docker Host
                              │
                              ▼
                      Workload Container
```

---

## Core Components

The Worker architecture is structured into decoupled, single-responsibility modules:

### 1. Registration (`forge.worker.registration`)

- **`WorkerRegistration`**: Immutable data structure advertising worker identity and capacity:
  - `name`: Unique worker identifier.
  - `version`: Worker software version.
  - `cpu_capacity`: Integer core count.
  - `memory_capacity_mb`: Total available memory in megabytes.
  - `gpu_capacity`: Integer GPU count.
  - `capabilities`: List of supported tags (e.g. `["docker", "python"]`).
- **`WorkerRegistrar`**: Sends `POST /api/v1/workers/register` to the Control Plane. Newly registered workers begin in `REGISTERING` state and receive an authoritative `worker_id` (UUID).

### 2. Heartbeat System (`forge.worker.heartbeat`)

- **`WorkerHeartbeat`**: Sends HTTP `POST /api/v1/workers/{worker_id}/heartbeat` with usage telemetry:
  - `cpu_usage`: Current CPU load.
  - `memory_usage_mb`: Current memory usage in MB.
  - `gpu_usage`: Current GPU load.
  - `running_attempts`: List of currently executing attempt UUIDs.
- **`HeartbeatLoop`**: Background async task executing heartbeats on a configured interval (`interval_seconds`).
  - Uses an internal `asyncio.Event` (`_stop_event`) with `asyncio.wait_for(...)` for interval timing.
  - Calling `loop.stop()` immediately unblocks the loop without waiting for the timeout to elapse, enabling instantaneous and clean shutdown.
- **`HeartbeatDataProvider`**: Callable producing the resource telemetry tuple `(cpu_usage, memory_usage_mb, gpu_usage, running_attempts)`.

### 3. Assignment Handling & Execution (`forge.worker.assignment*`)

- **`AssignmentDetailsClient`**: Fetches full execution context from the Control Plane via `GET /api/v1/workers/{worker_id}/assignments/{assignment_id}`.
- **`AssignmentHandler`**: Communicates assignment receipt and status updates.
- **`ExecutionRequestBuilder`**: Translates assignment details into a normalized `ExecutionRequest` (image, command, CPU/memory limits, environment variables).
- **`AssignmentExecutor`**: High-level coordinator that handles an incoming assignment:
  1. Fetches assignment details using `AssignmentDetailsClient`.
  2. Constructs the container execution request using `ExecutionRequestBuilder`.
  3. Executes the lifecycle via `WorkerExecutionLifecycle`.

### 4. Docker Execution (`forge.worker.executor.docker`)

- **`DockerExecutor`**: Implements the `Executor` interface against the local Docker daemon:
  - Validates and pulls container images.
  - Creates and starts containers with resource constraints:
    - `nano_cpus`: Converted from CPU requirements (`cpu * 10**9`).
    - `mem_limit`: Converted from MB requirements (`memory_mb * 1024 * 1024`).
  - Streams and collects stdout/stderr logs.
  - Waits for container exit and returns an `ExecutionResult` (exit code, logs, success boolean).

### 5. Attempt Lifecycle & Status Reporting (`forge.worker.status`, `forge.worker.lifecycle`)

- **`AttemptStatusReporter`**: Communicates status transitions to `POST /api/v1/attempts/{attempt_id}/status`.
- **`WorkerExecutionLifecycle`**: Enforces the attempt lifecycle states:
  - Reports `STARTING` prior to container launch.
  - Reports `RUNNING` once the container is active.
  - Reports terminal status (`SUCCEEDED`, `FAILED`, or `CANCELLED`) with the final `exit_code` and optional `error_message`.

### 6. Worker HTTP Server (`forge.worker.server`)

- **`create_worker_app(worker)`**: Creates an isolated FastAPI instance running on the Worker node.
- **`POST /assignments`**: Receives an `AssignmentDeliveryRequest(assignment_id=UUID)` from the Control Plane and invokes `worker.execute_assignment(assignment_id)`.

---

## Runtime Bootstrap & Process Entrypoint

### `create_worker()` (`forge.worker.runtime`)

`create_worker()` coordinates dependency injection and ordered initialization:

```text
create_worker()
    │
    ├─► 1. Construct WorkerRegistrar & Worker
    ├─► 2. await worker.register() -> establishes worker.id
    │       (fails with RuntimeError if worker.id is None)
    ├─► 3. Construct ID-dependent components (AssignmentDetailsClient,
    │       AssignmentHandler, WorkerHeartbeat)
    ├─► 4. Construct Executor, StatusReporter, Lifecycle, AssignmentExecutor
    ├─► 5. Construct HeartbeatLoop
    └─► 6. worker.initialize_runtime(assignment_executor, heartbeat_loop)
```

### Process Entrypoint (`forge.worker.main`)

- **`WorkerConfig`**: Reads configuration from environment variables with safe defaults:
  - `FORGE_API_URL`: Control Plane URL (default: `http://localhost:8000`).
  - `WORKER_NAME`, `WORKER_VERSION`: Identification metadata.
  - `WORKER_CPU_CAPACITY`, `WORKER_MEMORY_CAPACITY_MB`, `WORKER_GPU_CAPACITY`: Node hardware limits.
  - `WORKER_CAPABILITIES`: Comma-separated tags (e.g. `docker,python`).
  - `WORKER_HEARTBEAT_INTERVAL_SECONDS`: Heartbeat cadence (default: `10.0`).
  - `WORKER_HOST`, `WORKER_PORT`: Worker HTTP bind address (default: `0.0.0.0:8001`).
- **`run_worker()`**:
  1. Bootstraps the Worker via `create_worker()`.
  2. Constructs the Worker HTTP application via `create_worker_app(worker)`.
  3. Spawns `worker.run_heartbeat()` and Uvicorn `server_runner` concurrently.
  4. On completion or uncaught exception, triggers clean shutdown: invokes `worker.stop()`, cancels background tasks, and awaits task termination.
- **`main()`**: CLI entrypoint executing `asyncio.run(run_worker())`.

---

## Architectural Isolation

1. **Separation of Control Plane & Worker**:
   - The Worker FastAPI app (`create_worker_app`) is completely distinct from the Forge Control Plane API (`forge.api.main`).
   - The Worker contains no direct database connections (PostgreSQL) and no direct message broker connections (Redis Streams). All control plane interaction occurs over HTTP.

2. **No Worker-Side Scheduling**:
   - Workers never select jobs, query database queues, or allocate their own resources.
   - Workers strictly execute assignments dispatched by the Scheduler.

---

## Phase Boundaries & Deferred Concerns

- **Phase 5 (Current)** implements worker registration, heartbeat transmission, push-based assignment reception, Docker execution with resource limits, attempt status updates, runtime bootstrapping, and process lifecycle.
- **Deferred Concerns**:
  - Authentication and mTLS between Control Plane and Worker.
  - Sophisticated OS-level live resource monitoring (currently uses default reporting structure).
  - Worker draining / graceful job migration protocols.
  - Pull-based execution models.

