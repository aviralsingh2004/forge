# Forge — v0.1 Project Specification

## 1. Project Purpose

Forge is a learning-focused distributed job scheduling and execution platform.

The primary goal is not merely to produce a working scheduler, but to learn how an industry-grade distributed backend can be designed, implemented, operated, tested, and evolved.

Forge should progressively expose concepts including:

- Backend development with FastAPI
- PostgreSQL
- Redis Streams
- Distributed scheduling
- Resource-aware scheduling
- Worker management
- Heartbeats and failure detection
- Docker-based workload execution
- Resource reservations
- Concurrency control
- Nginx reverse proxying
- Configuration management
- confd
- Docker Compose
- Linux/WSL2 integration
- GPU workloads
- Observability
- Scaling
- Eventually Kubernetes

The project is intentionally incremental.

Do not introduce technologies merely because they are commonly used in production. Introduce them when Forge has a concrete problem that they solve.

---

# 2. Development Environment

Primary development machine:

- Windows
- 16 GB RAM
- SSD
- NVIDIA GTX 1650 GPU

Linux-oriented development will use:

- WSL2
- Docker Desktop
- Linux containers

The project must remain practical on a 16 GB RAM laptop.

Avoid unnecessarily running large numbers of services or replicas.

---

# 3. High-Level Architecture

Initial architecture:

```text
                         Client
                           |
                           v
                         Nginx
                           |
                           v
                        FastAPI
                           |
              +------------+------------+
              |                         |
              v                         v
         PostgreSQL                  Outbox
              |                         |
              |                         v
              |                    Redis Streams
              |                         |
              |                         v
              |                      Scheduler
              |                         |
              +-------------------------+
                                        |
                                        | HTTP Push
                                        v
                                      Worker
                                        |
                                        v
                                      Docker
                                        |
                                        v
                                    Container
```

The frontend will be developed later.

The initial system is backend/control-plane focused.

---

# 4. Core Architectural Principles

## 4.1 PostgreSQL is the source of truth

PostgreSQL owns durable business state.

This includes:

- Jobs
- Attempts
- Workers
- Assignments
- Reservations
- Heartbeats/history
- Job events
- Outbox events

Redis must not become the authoritative source for these states.

---

## 4.2 Redis Streams is the scheduling event mechanism

Redis Streams is used from v0.1.

Initial stream:

```text
forge:scheduling
```

Initial consumer group:

```text
scheduler-group
```

Redis is responsible for:

- scheduling events
- delivery
- consumer groups
- pending messages
- at-least-once event processing

Redis is NOT responsible for authoritative job/resource state.

RabbitMQ is explicitly deferred.

Do not introduce RabbitMQ in v0.1.

---

## 4.3 Scheduler owns scheduling decisions

The Scheduler decides:

- which Job should run
- which Worker should execute it
- resource allocation
- reservations
- assignment creation

Workers do not choose Jobs.

Workers do not choose their own allocations.

---

## 4.4 Workers own execution

Workers are responsible for:

- registration
- heartbeat
- receiving assignments
- starting Docker containers
- enforcing assigned resources
- reporting execution status
- reporting actual resource usage

Workers do not directly modify scheduler reservations.

---

## 4.5 Push-based worker architecture

Forge v0.1 uses push-based assignments.

```text
Scheduler
    |
    | HTTP POST
    v
Worker
```

Do not implement pull-based workers in v0.1.

Pull-based workers may be explored in a later version.

---

# 5. Job Model

A Job represents a user's requested workload.

For v0.1, the Job explicitly declares its resource requirements.

Example:

```json
{
  "name": "image-processing",
  "image": "python:3.12",
  "command": ["python", "process.py"],
  "priority": 80,
  "resources": {
    "cpu": 2,
    "memory_mb": 2048,
    "gpu": 0
  },
  "capabilities": ["python"],
  "max_retries": 3
}
```

Do not implement automatic resource prediction in v0.1.

Future versions may support profiles such as:

```text
small
medium
large
gpu
```

where Forge determines actual resource requirements using historical/observed data.

---

# 6. Job States

Initial Job states:

```text
QUEUED
ASSIGNED
RUNNING
SUCCEEDED
FAILED
CANCELLED
```

Future states may be added only when necessary.

---

# 7. Job Scheduling Order

Initial ordering:

```text
priority DESC
created_at ASC
```

Higher priority runs first.

For equal priority, older Jobs run first.

This is priority + FIFO scheduling.

---

# 8. Worker Model

Workers advertise:

```text
CPU capacity
Memory capacity
GPU capacity
Capabilities
```

Example:

```json
{
  "name": "worker-01",
  "resources": {
    "cpu": 8,
    "memory_mb": 16384,
    "gpu": 1
  },
  "capabilities": [
    "docker",
    "python",
    "cuda"
  ]
}
```

Worker statuses initially include:

```text
REGISTERING
READY
BUSY
DRAINING
OFFLINE
```

BUSY does not automatically make a Worker ineligible.

A BUSY Worker may receive another Job if it has sufficient available resources.

A DRAINING Worker must not receive new work.

---

# 9. Worker Health

Worker liveness is determined using heartbeats.

Worker sends periodic heartbeat:

```http
POST /api/v1/workers/{worker_id}/heartbeat
```

Heartbeat includes current usage information.

Example:

```json
{
  "timestamp": "...",
  "resources": {
    "cpu_usage": 1.4,
    "memory_usage_mb": 3240,
    "gpu_usage": 0.2
  },
  "running_attempts": [
    "attempt_123"
  ]
}
```

The scheduler uses health information to determine eligibility.

---

# 10. Resource Allocation

v0.1 uses explicit resource reservations.

Example Worker:

```text
CPU: 8
RAM: 16 GB
GPU: 1
```

Reservation:

```text
CPU: 2
RAM: 2 GB
GPU: 0
```

Available resources are initially calculated from active reservations:

```text
available = capacity - active reservations
```

Do not prematurely add cached aggregate fields such as `reserved_cpu`.

Those may be introduced later as an optimization and benchmarked.

---

# 11. Scheduler

Initial Scheduler is:

- priority-aware
- FIFO-aware
- resource-aware
- capability-aware
- health-aware
- load-aware
- fragmentation-aware

It is NOT:

- adaptive
- historical
- ML-based
- predictive

The Scheduler flow:

```text
Receive scheduling event
        |
        v
Find schedulable Jobs
        |
        v
Order by priority DESC, created_at ASC
        |
        v
Claim Job
        |
        v
Find READY/eligible Workers
        |
        v
Filter Workers
        |
        +--> resource requirements fit
        +--> capabilities fit
        +--> not draining
        +--> healthy
        |
        v
Score eligible Workers
        |
        +--> resource fit
        +--> resource pressure
        +--> fragmentation
        +--> load
        |
        v
Select best Worker
        |
        v
Create Attempt
        |
        v
Create Reservation
        |
        v
Create Assignment
        |
        v
Push Assignment to Worker
```

---

# 12.1 Worker scoring

Initial scoring should consider:

```text
resource fit
load
fragmentation
resource pressure
```

The exact numerical weights should be configurable.

A reasonable initial implementation can use:

```text
50% resource fit
30% load
20% fragmentation
```

but this is not a permanent algorithm.

The implementation should make the scoring logic isolated and easy to change.

---

# 13. Database Transaction During Scheduling

Resource allocation must be transactional.

Conceptually:

```text
BEGIN

lock Worker

lock Job

recalculate available resources

verify Job is still QUEUED

verify resources still fit

create Attempt

create Reservation

create Assignment

update Job

create JobEvent

COMMIT
```

The database must protect against two Scheduler instances allocating the same resources simultaneously.

---

# 14. Concurrency Requirement

Example:

Worker:

```text
8 CPU
```

Two Jobs:

```text
Job A = 6 CPU
Job B = 6 CPU
```

Two Scheduler instances may race.

Expected result:

```text
Job A → ASSIGNED
Job B → QUEUED
```

Never allow:

```text
6 + 6 > 8
```

The concurrency test for this behavior is mandatory.

---

# 15. Database Tables

Initial tables:

```text
workers
jobs
attempts
assignments
reservations
worker_heartbeats
job_events
outbox_events
```

---

# 16. Workers Table

Conceptually:

```text
workers
---------
id
name
status
version

cpu_capacity
memory_capacity_mb
gpu_capacity

capabilities

last_heartbeat_at

created_at
updated_at
```

Capabilities may initially be stored as PostgreSQL JSONB.

---

# 17. Jobs Table

Conceptually:

```text
jobs
---------
id
name

image
command

priority

cpu_required
memory_required_mb
gpu_required

required_capabilities

status

max_retries

created_at
updated_at
```

Use explicit resource columns for v0.1.

Do not prematurely generalize resources into arbitrary structures.

---

# 18. Attempts

An Attempt represents one execution attempt of a Job.

```text
attempts
---------
id
job_id

attempt_number

worker_id

status

started_at
finished_at

exit_code
error_message

created_at
updated_at
```

A Job can have multiple Attempts due to retries.

Example:

```text
Job 123

Attempt 1 → FAILED
Attempt 2 → FAILED
Attempt 3 → SUCCEEDED
```

---

# 19. Attempt States

Initial states:

```text
CREATED
ASSIGNED
STARTING
RUNNING
SUCCEEDED
FAILED
CANCELLED
```

---

# 20. Assignments

Assignment represents the Scheduler's instruction to a Worker.

```text
assignments
------------
id
attempt_id
worker_id

status

created_at
delivered_at
acknowledged_at
completed_at
```

States:

```text
CREATED
DELIVERED
ACKNOWLEDGED
COMPLETED
FAILED
CANCELLED
```

---

# 21. Reservations

Reservations represent logical resource allocation.

```text
reservations
-------------
id

worker_id
attempt_id

cpu_reserved
memory_reserved_mb
gpu_reserved

status

created_at
released_at
```

Only one active reservation should exist for an Attempt.

The database should enforce important invariants where possible.

---

# 22. Worker Heartbeats

Maintain:

```text
workers.last_heartbeat_at
```

for fast health checks.

Also maintain heartbeat history:

```text
worker_heartbeats
------------------
id
worker_id
timestamp

cpu_usage
memory_usage_mb
gpu_usage

running_attempts
```

Old heartbeat history may eventually be cleaned up.

---

# 23. Job Events

Maintain an audit/event history:

```text
job_events
----------
id
job_id
attempt_id

event_type
metadata

created_at
```

Examples:

```text
JOB_CREATED
JOB_QUEUED
JOB_ASSIGNED
ATTEMPT_CREATED
ATTEMPT_STARTED
ATTEMPT_SUCCEEDED
ATTEMPT_FAILED
JOB_CANCELLED
JOB_RETRYING
```

This is important for debugging and future observability.

---

# 24. Outbox Events

The Outbox Pattern is mandatory for v0.1.

Reason:

Creating a Job involves both PostgreSQL and Redis.

We must avoid this unsafe dual-write:

```text
PostgreSQL succeeds
Redis fails
```

Instead:

```text
BEGIN

INSERT Job
INSERT OutboxEvent

COMMIT
```

Then a separate Outbox Publisher:

```text
PostgreSQL Outbox
        |
        v
Outbox Publisher
        |
        v
Redis XADD
```

If Redis is unavailable, the event remains pending and can be retried.

---

# 25. Redis Streams

Initial stream:

```text
forge:scheduling
```

Initial consumer group:

```text
scheduler-group
```

Scheduling events should contain enough information for the Scheduler to find authoritative state in PostgreSQL.

Redis events must not become the authoritative Job state.

---

# 26. Failure Behavior

## Redis failure

If Redis is unavailable:

```text
Job remains QUEUED
OutboxEvent remains pending
```

Once Redis returns:

```text
Outbox Publisher retries
```

---

## Scheduler failure

If Scheduler crashes while processing a Redis message:

The message remains pending.

Another Scheduler may process it.

Processing must be idempotent.

---

## Scheduler crash after PostgreSQL commit

Example:

```text
PostgreSQL:
Job = ASSIGNED
Reservation = ACTIVE
Assignment = CREATED

Scheduler crashes
```

A new Scheduler must detect that the Job is already assigned and must not allocate it again.

---

## Worker failure

If Worker disappears:

```text
heartbeat timeout
```

The control plane eventually detects the failure.

Future implementation will determine whether an Attempt is retried.

---

# 27. API Contract

Base path:

```text
/api/v1
```

---

## Create Job

```http
POST /api/v1/jobs
```

Request:

```json
{
  "name": "image-processing",
  "image": "python:3.12",
  "command": ["python", "process.py"],
  "priority": 80,
  "resources": {
    "cpu": 2,
    "memory_mb": 2048,
    "gpu": 0
  },
  "capabilities": ["python"],
  "max_retries": 3
}
```

Response:

```json
{
  "id": "job_123",
  "status": "QUEUED",
  "created_at": "...",
  "updated_at": "..."
}
```

The API must not choose a Worker.

---

## Get Job

```http
GET /api/v1/jobs/{job_id}
```

Should return:

- Job information
- current status
- resource requirements
- Attempts
- Worker associated with active Attempt

---

## Cancel Job

```http
POST /api/v1/jobs/{job_id}/cancel
```

Cancellation changes execution state.

Do not delete the Job.

---

# 28. Worker Registration API

```http
POST /api/v1/workers/register
```

Request:

```json
{
  "name": "worker-01",
  "resources": {
    "cpu": 8,
    "memory_mb": 16384,
    "gpu": 1
  },
  "capabilities": [
    "docker",
    "python",
    "cuda"
  ]
}
```

Response:

```json
{
  "worker_id": "worker_01",
  "status": "REGISTERING",
  "heartbeat_interval_seconds": 10
}
```

---

# 29. Worker Heartbeat API

```http
POST /api/v1/workers/{worker_id}/heartbeat
```

Worker reports:

- health
- actual resource usage
- running Attempts

---

# 30. Worker Assignment API

Scheduler pushes:

```http
POST /api/v1/workers/{worker_id}/assignments
```

Example:

```json
{
  "assignment_id": "assignment_123",
  "attempt_id": "attempt_123",
  "job_id": "job_123",

  "image": "python:3.12",

  "command": ["python", "process.py"],

  "resources": {
    "cpu": 2,
    "memory_mb": 2048,
    "gpu": 0
  }
}
```

Worker responds with acceptance.

---

# 31. Attempt Status API

```http
POST /api/v1/attempts/{attempt_id}/status
```

Example:

```json
{
  "status": "SUCCEEDED",
  "exit_code": 0,
  "finished_at": "..."
}
```

Worker reports execution status.

The control plane owns the resulting Job/Reservation state transitions.

---

# 32. Health APIs

```text
GET /health/live
GET /health/ready
```

`/live` answers:

> Is the process alive?

`/ready` checks required dependencies such as PostgreSQL and Redis.

---

# 33. State Ownership

The following ownership rules are mandatory:

| Concern | Owner |
|---|---|
| Job creation | API |
| Job resource requirements | API |
| Job priority | API |
| Job scheduling | Scheduler |
| Resource reservation | Scheduler/control plane |
| Assignment creation | Scheduler |
| Container execution | Worker |
| Attempt execution status | Worker |
| Actual resource usage | Worker |
| Reservation release | Control plane |
| Final Job state | Control plane |

Workers must not directly manipulate reservations.

---

# 34. Docker Execution

Worker eventually translates Job requirements into Docker constraints.

For example:

```text
CPU
Memory
GPU
```

CPU and memory execution must work independently of GPU availability.

GPU execution is optional.

The NVIDIA GTX 1650 can be used when the Windows + WSL2 + Docker Desktop + NVIDIA driver stack supports GPU passthrough.

Do not make GPU support a blocker for CPU-only Forge functionality.

---

# 35. Nginx

Nginx will eventually provide:

```text
Client
   |
   v
Nginx
   |
   +---- /api/* ---> FastAPI
   |
   +---- /* -------> React
```

React is not required for the initial backend milestone.

---

# 36. Configuration

Configuration should use environment variables initially.

Later, confd can be introduced where it solves an actual configuration problem.

Potential configurable values:

```text
scheduler weights
heartbeat interval
timeouts
worker configuration
Nginx upstream configuration
```

Avoid building a custom configuration CLI before the underlying configuration system exists.

---

# 37. Docker Compose

Docker Compose is the primary local orchestration mechanism for v0.1.

Initial services:

```text
postgres
redis
nginx
api
scheduler
worker
```

Keep the number of replicas low due to the 16 GB RAM development machine.

Docker Compose profiles may later be used for optional components:

```text
default
gpu
monitoring
debug
```

---

# 38. Kubernetes

Kubernetes is explicitly deferred.

Do not introduce Kubernetes until Forge works correctly using Docker Compose.

Later Kubernetes will be used as a learning exercise and comparison point:

```text
Forge Scheduler
        vs
Kubernetes Scheduler
```

---

# 39. RabbitMQ

RabbitMQ is explicitly deferred.

Do not install or integrate RabbitMQ in v0.1.

Later, if Forge encounters a messaging requirement where RabbitMQ provides meaningful advantages over Redis Streams, introduce it and compare the architectures.

---

# 40. Frontend

React will be developed later.

Backend APIs should therefore be designed cleanly enough that a React client can consume them.

Frontend work can begin once the primary APIs are stable, but it should not block scheduler/backend development.

---

# 41. v0.1 Implementation Phases

Forge should be implemented as a sequence of small, verifiable milestones. Each
phase should leave the repository runnable and should add tests before the next
distributed-system boundary is introduced. PostgreSQL remains the source of
truth throughout the plan; Redis Streams carries scheduling events but does not
own business state.

## Phase 0 — Repository and Tooling

### Objectives

Create a reproducible repository and local development environment.

### Work

1. Initialize Git with `git init`.
2. Create the project layout:

```text
forge/
├── backend/
├── frontend/
├── deploy/
├── docs/
├── docker-compose.yml
├── .env
├── .env.example
├── README.md
└── Makefile
```

3. Set up Python with `uv` or Poetry, using a supported Python version.
4. Configure the package, test discovery, and import paths.
5. Add Ruff, pytest, pytest-asyncio, and pre-commit.
6. Add Docker and Docker Compose configuration for local services.
7. Put `.env` and other secret-bearing files in `.gitignore`.
8. Document every required environment variable in `.env.example` without
  committing real credentials.
9. Add basic commands for install, lint, format, test, migration, and Compose
  startup.

### Verification gate

The following should work from a clean checkout:

```text
uv sync
uv run ruff check .
uv run pytest
docker compose up
```

`docker compose up` must start the infrastructure services without requiring
secrets in Git. Application containers may be added as their phases are ready.

## Phase 1 — PostgreSQL Persistence

### Objectives

Build the durable domain model and establish PostgreSQL as the authoritative
state store.

### Work

1. Configure environment-backed PostgreSQL settings and an async SQLAlchemy
  engine/session factory.
2. Define the initial tables:

```text
workers
jobs
attempts
assignments
reservations
worker_heartbeats
job_events
outbox_events
```

3. Add the state enums and timestamp fields.
4. Add foreign keys with deliberate delete behavior.
5. Add CHECK constraints for non-negative resources, valid priorities, retry
  counts, and valid attempt numbers.
6. Add unique constraints for worker names, attempt numbers per Job, and one
  active reservation or assignment where required.
7. Add indexes for status, scheduling order, worker lookup, and event history.
8. Create and verify the first Alembic migration.
9. Document how to upgrade, downgrade, and inspect the schema.

### Tests and verification

- Create and retrieve a `Worker` and a `Job`.
- Verify relationships and enum defaults.
- Verify constraint and uniqueness failures.
- Run the tests against PostgreSQL rather than replacing the database with an
  in-memory substitute.

## Phase 2 — Redis Streams

### Objectives

Introduce Redis as the scheduling event mechanism while keeping PostgreSQL as
the source of truth.

### Work

1. Configure an async Redis client from environment settings.
2. Use the initial stream:

```text
forge:scheduling
```

3. Create the consumer group:

```text
scheduler-group
```

4. Implement event serialization and publishing with event ID, type, Job ID,
  and creation time.
5. Implement consumer-group reads, acknowledgements, pending-message lookup,
  and clean shutdown.
6. Keep Redis connection and health failures explicit and observable.

### Tests and verification

- Publish and read a scheduling event.
- Verify consumer-group acknowledgement.
- Verify pending-message inspection.
- Verify Redis-unavailable behavior.
- Do not store authoritative Job, Worker, reservation, or assignment state in
  Redis.

## Phase 3 — Outbox Publisher

### Objectives

Guarantee that a committed business change has a corresponding scheduling
event without publishing to Redis inside the request transaction.

### Work

1. Keep `OutboxEvent` in PostgreSQL with publication state, retry count, and
  last error.
2. Add helpers that append an outbox row to the caller's transaction.
3. Implement the Outbox Publisher as a separate process or service loop.
4. Select unpublished events in creation order.
5. Publish the event to Redis only after the PostgreSQL transaction commits.
6. Mark successful events as published.
7. Record failures and implement bounded retry/backoff behavior.
8. Make publishing idempotent enough to tolerate process restarts and
  at-least-once delivery.

### Tests and verification

- Job and OutboxEvent commit together.
- A transaction rollback removes both records.
- The API does not call Redis directly.
- Successful publication marks the event published.
- Failed publication retains the event and records retry information.

## Phase 4 — FastAPI Control Plane

### Objectives

Expose the first HTTP API while keeping handlers small and explicit.

### Work order

1. Create the FastAPI application and route registration.
2. Add liveness and PostgreSQL readiness endpoints.
3. Add Job request/response schemas with explicit validation.
4. Implement Job creation transactionally with its OutboxEvent.
5. Implement Job retrieval and ordered Job listing.
6. Implement partial Job updates with a `JobEvent` and `OutboxEvent`.
7. Implement Job cancellation only after the state-transition rules are
  defined and tested.
8. Add worker registration and worker lookup endpoints.
9. Add heartbeat handling.
10. Add assignment delivery and attempt status endpoints only after the Worker
   contract is stable.

### Initial API surface

```text
POST /api/v1/jobs
GET  /api/v1/jobs
GET  /api/v1/jobs/{job_id}
PATCH /api/v1/jobs/{job_id}
POST /api/v1/jobs/{job_id}/cancel

GET  /api/v1/workers
GET  /api/v1/workers/{worker_id}
POST /api/v1/workers/register
POST /api/v1/workers/{worker_id}/heartbeat
POST /api/v1/workers/{worker_id}/assignments
POST /api/v1/attempts/{attempt_id}/status

GET /health/live
GET /health/ready
```

### Tests and verification

- Exercise every endpoint through FastAPI's HTTP test client.
- Use PostgreSQL integration tests for persistence and transaction checks.
- Verify validation errors do not create database records.
- Verify read-only endpoints do not create events or modify state.
- Verify API requests do not publish directly to Redis.
- Verify health endpoints return `200` for healthy dependencies and `503` for
  unavailable required dependencies.

## Phase 5 — Worker Process

### Objectives

Build the first executable Worker and define the push-based execution
contract.

### Work

1. Create a Worker process with its own configuration and lifecycle.
2. Register the Worker with capacity, capabilities, version, and status.
3. Return the Worker ID and heartbeat interval from registration.
4. Run a heartbeat loop and report capacity, usage, and active attempts.
5. Receive assignments through HTTP push.
6. Validate an assignment before execution and acknowledge its state.
7. Report attempt start, running, success, failure, and cancellation states.
8. Shut down cleanly without abandoning local execution state.

### Verification gate

One Worker can register, send heartbeats, receive a test assignment, and report
the assignment lifecycle without a Scheduler choosing work locally.

## Phase 6 — Scheduler v0.1

### Objectives

Build the scheduling decision path from Redis event to transactional database
assignment.

### Work

Create the scheduler as focused modules:

```text
scheduler/
├── consumer.py
├── engine.py
├── job_selector.py
├── worker_filter.py
├── scorer.py
├── allocator.py
└── recovery.py
```

1. Consume scheduling events from `forge:scheduling` using
  `scheduler-group`.
2. Select only Jobs that are still `QUEUED`.
3. Order Jobs by priority and FIFO creation time.
4. Filter Workers by health, status, capabilities, and available resources.
5. Score feasible Workers using the initial configurable weights:

```text
resource fit: 50%
load:          30%
fragmentation: 20%
```

6. Pass the selected Job and Worker to the allocator.
7. Acknowledge the Redis event only after the database decision is safely
  committed or is known to be obsolete.

## Phase 7 — Transactional Resource Allocation

### Objectives

Prevent concurrent Scheduler instances from over-allocating a Worker.

### Allocation transaction

Implement the following inside one PostgreSQL transaction:

```text
BEGIN
  lock Worker
  lock Job
  recalculate available resources
  verify Job is still QUEUED
  verify resources still fit
  create Attempt
  create Reservation
  create Assignment
  update Job
  create JobEvent
COMMIT
```

The transaction must be safe when two Scheduler instances compete for the same
Worker. Reservations must be derived from committed database state, not cached
Redis state.

### Concurrency gate

With one Worker providing 8 CPU and two Jobs requiring 6 CPU each, two
simultaneous allocation attempts must produce:

```text
one Job: ASSIGNED
one Job: QUEUED
```

The system must never reserve 12 CPU on an 8 CPU Worker.

## Phase 8 — Docker Executor

### Objectives

Execute assignments on the Worker with explicit resource limits.

### Work

1. Use a Docker SDK or equivalent supported client.
2. Translate the assignment image and command into a container.
3. Apply CPU and memory limits.
4. Add GPU configuration only when the Worker advertises GPU capability and the
  Job requires it.
5. Capture exit code, output metadata, and failure reason.
6. Ensure containers are cleaned up after completion.
7. Keep `gpu_required = 0` fully functional without GPU support.

GPU execution depends on the Windows NVIDIA driver, WSL2 GPU support, and Docker
Desktop integration. CPU and RAM execution must not depend on that setup.

## Phase 9 — Reliability and Recovery

### Objectives

Make delivery and execution behavior safe under process and infrastructure
failures.

### Work

1. Make event handling idempotent.
2. Recover pending Redis messages after Scheduler failure.
3. Handle a Scheduler crash before and after a database commit.
4. Detect Worker heartbeat expiry and mark Workers unavailable.
5. Retry assignment delivery when a Worker is unreachable.
6. Keep failed or stale assignments visible for recovery.
7. Release reservations on terminal attempt states and detected Worker loss.
8. Prevent duplicate Attempts, Reservations, Assignments, and state transitions.

### Failure tests

Test at minimum:

```text
Redis unavailable
PostgreSQL unavailable
Scheduler crash before commit
Scheduler crash after commit
Worker unavailable
Worker heartbeat timeout
Worker assignment HTTP failure
Container failure
```

## Phase 10 — Nginx and Compose Topology

### Objectives

Move from direct service ports to a production-like local topology.

### Work

1. Put Nginx in front of FastAPI.
2. Proxy `/api/*` and health endpoints to the API service.
3. Preserve useful request and upstream error logs.
4. Verify:

```text
localhost/api/v1/...
localhost/health/live
localhost/health/ready
```

5. Keep Docker Compose as the primary local orchestration mechanism.
6. Add profiles only when components exist and have a real operational use:

```text
default
gpu
monitoring
debug
```

7. Do not introduce Kubernetes before the complete local Compose flow is
  reliable.

## Phase 11 — Configuration Management

Introduce `confd` only when dynamic configuration solves a demonstrated problem.
Potential configuration targets include Scheduler weights, Worker heartbeat
intervals, resource policies, and Nginx upstreams. The configuration flow
should be:

```text
configuration source
      |
      v
    confd
      |
      v
generated configuration
      |
      v
 Forge or Nginx
```

Do not build a custom enable/disable CLI before the underlying configuration
and Compose profiles are useful independently.

## Phase 12 — End-to-End v0.1 Milestone

The complete local demonstration must work:

```text
Submit Job
   |
   v
FastAPI
   |
   v
PostgreSQL
   |
   v
Outbox
   |
   v
Redis Streams
   |
   v
Scheduler
   |
   v
Worker selection
   |
   v
Reservation
   |
   v
HTTP assignment
   |
   v
Worker
   |
   v
Docker
   |
   v
Container
   |
   v
Completion
   |
   v
Reservation release
   |
   v
Job SUCCEEDED
```

This is the primary v0.1 milestone.

---

# 42. Testing Requirements

Tests should be written continuously rather than at the end.

Important test categories:

### Unit

- Job selection
- Worker filtering
- Worker scoring
- Resource calculations
- state transitions

### Integration

- PostgreSQL
- Redis Streams
- Outbox
- FastAPI
- Worker

### Concurrency

At least:

```text
2 Scheduler instances
1 Worker
2 competing Jobs
```

Verify resource safety.

### Failure

Test:

```text
Redis unavailable
PostgreSQL unavailable
Scheduler crash
Worker crash
Container failure
```

---

# 43. Learning Rule

Forge is a learning project.

When implementing a component:

1. Prefer straightforward code.
2. Avoid unnecessary abstractions.
3. Explain non-obvious design decisions.
4. Add tests for important invariants.
5. Avoid adding dependencies without justification.
6. Do not hide distributed-system behavior behind excessive frameworks.
7. Prefer understanding the mechanism over using a black box.

If a production-grade optimization is deliberately postponed, document it rather than silently implementing it.

---

# 44. Codex Working Rules

Codex should treat this document as the architectural source of truth.

Codex should:

- implement incrementally
- keep changes focused
- write tests alongside code
- run tests after changes
- avoid unrelated refactoring
- avoid introducing technologies not specified here
- explain architectural changes
- preserve PostgreSQL as source of truth
- preserve Redis Streams as the scheduling event mechanism
- preserve push-based Workers
- preserve explicit Job resource requirements
- preserve resource-aware priority scheduling

Codex should NOT:

- introduce RabbitMQ
- introduce Kubernetes
- introduce Kafka
- introduce ML-based scheduling
- introduce adaptive/historical scheduling
- introduce unnecessary cloud services
- replace Redis Streams with another queue
- replace PostgreSQL with another database
- add authentication unless explicitly requested
- over-engineer v0.1

---

# 45. Definition of Done for v0.1

Forge v0.1 is complete when:

1. A Job can be submitted through FastAPI.
2. Job state is persisted in PostgreSQL.
3. A scheduling event reaches Redis Streams.
4. Scheduler consumes the event.
5. Scheduler selects an eligible Worker.
6. Scheduler performs a transactional resource reservation.
7. Scheduler creates an Assignment.
8. Assignment is pushed to the Worker.
9. Worker starts a Docker container.
10. Docker resource limits are applied.
11. Worker reports execution status.
12. Forge releases the reservation.
13. Job reaches a terminal state.
14. Redis/Scheduler/Worker failure cases have tests.
15. Multiple Scheduler instances cannot over-allocate Worker resources.
16. Nginx can proxy the API.
17. The entire system can run locally using Docker Compose.

The final v0.1 demonstration should be:

```text
curl
  |
  v
Nginx
  |
  v
FastAPI
  |
  v
PostgreSQL + Outbox
  |
  v
Redis Streams
  |
  v
Scheduler
  |
  v
Worker
  |
  v
Docker Container
  |
  v
SUCCESS
```

This is the baseline from which future Forge versions will evolve.

# Deferred / Revisit Later

Forge v0.1 intentionally simplifies or postpones several features because the
components and reliability mechanisms they depend on are not yet implemented.
These items are deferred deliberately, not forgotten.

## 1. Job Cancellation Beyond QUEUED

### Current v0.1 behavior

- `QUEUED -> CANCELLED` is supported.
- `ASSIGNED`, `RUNNING`, `SUCCEEDED`, `FAILED`, and `CANCELLED` Jobs cannot
  currently be cancelled and return `409 Conflict`.

### Reason for deferral

`ASSIGNED` and `RUNNING` cancellation requires Worker coordination and, for
running Jobs, Docker/container lifecycle control.

### Revisit when

- Worker execution exists.
- The Docker executor exists.
- Assignment and Attempt lifecycles are implemented.

### Expected future flow

```text
Cancel request
  |
  v
Control Plane
  |
  v
Worker
  |
  v
Stop Docker container
  |
  v
Update Attempt/Assignment
  |
  v
Release Reservation
  |
  v
Job CANCELLED
```

## 2. Worker Registration Enhancements

### Current v0.1 behavior

- Worker registration creates a Worker in PostgreSQL.
- Newly registered Workers start in `REGISTERING`.
- Registration does not automatically transition a Worker to `READY`.

### Deferred concerns

- Worker authentication and identity verification.
- Registration tokens or credentials.
- Automatic readiness validation.
- Docker availability verification.
- Machine and resource discovery.
- Capability verification.
- Re-registration and reconciliation of an existing Worker.

### Revisit when

The actual Worker agent is implemented in Phase 5.

## 3. Duplicate Worker Registration API Semantics

### Current v0.1 behavior

Worker names are protected by a PostgreSQL unique constraint. Duplicate names
currently rely on that database constraint.

### Deferred

- Decide whether the API should explicitly translate duplicate registration
  into a clean `409 Conflict`.
- Define Worker identity and re-registration semantics once real Worker agents
  register automatically.

## 4. Outbox Duplicate Publication / Event Idempotency

### Current v0.1 behavior

- Outbox events have unique event IDs.
- The Outbox Publisher tracks `published_at`, `retry_count`, and `last_error`.
- Publication is retried through later publisher runs.
- Full idempotent delivery is not implemented yet.

### Known failure scenario

```text
Outbox event
  |
  v
Redis publish succeeds
  |
  v
Process crashes before published_at is persisted
  |
  v
Event may be published again
```

### Deferred

- Consumer-side event idempotency.
- Duplicate event detection using `event_id`.
- Safe handling of duplicate scheduling events.
- Stronger Outbox claiming and locking if multiple Publishers are introduced.
- Retry backoff and retry policy.

### Revisit in

Phase 7 — Reliability.

## 5. Redis Failure Recovery

### Current v0.1 behavior

Redis Streams provides asynchronous event delivery. Basic connection,
publishing, consumer groups, acknowledgement, and pending-message handling
exist.

### Deferred

- Robust Redis outage recovery.
- Consumer reconnection strategy.
- Publisher recovery.
- Pending-message claiming and reprocessing policy.
- Backoff and retry policies.
- Handling Redis recovery while the Scheduler is running.

### Revisit in

Phase 7 — Reliability.

## 6. Scheduler Failure Recovery

### Current v0.1 behavior

The Scheduler is responsible for consuming scheduling events and making
scheduling decisions.

### Deferred

- Scheduler crash recovery.
- Multiple Scheduler instances.
- Concurrent scheduling coordination.
- Recovery of partially completed scheduling operations.
- Reconciliation of Jobs, Attempts, Assignments, and Reservations after
  Scheduler failure.

### Revisit in

Phase 7 — Reliability.

## 7. Worker Failure Detection and Stale State

### Current v0.1 behavior

- Workers have heartbeat support.
- `last_heartbeat_at` is stored.

### Deferred

- Heartbeat timeout policy.
- Transitioning Workers to `OFFLINE`.
- Detecting stale Assignments.
- Recovering Jobs from failed Workers.
- Releasing resources after Worker failure.
- Reassigning interrupted work according to retry policy.

### Revisit in

Phase 7 — Reliability.

## 8. Running Job Cancellation and Docker Lifecycle

### Current v0.1 behavior

Running-Job cancellation is not implemented.

### Deferred

- Sending cancellation commands to Workers.
- Docker container termination.
- Graceful versus forced container shutdown.
- Cancellation timeouts.
- Correct Attempt and Assignment state transitions.
- Reservation release after cancellation.

### Revisit when

- Phase 5 Worker and Docker execution are implemented.
- Phase 7 Reliability is implemented.

## 9. Docker Executor Enhancements

### Current v0.1 goal

The Worker will execute Jobs through a Docker executor.

### Future/deferred concerns

- CPU limits.
- Memory limits.
- GPU access.
- Container lifecycle management.
- Exit-code handling.
- stdout/stderr collection.
- Execution timeouts.
- Container cleanup.
- Executor failures.
- Image-pull failures.
- Docker daemon failures.

These should be implemented progressively during Phase 5 and hardened during
Phase 7.

## 10. Scheduler Concurrency and Resource Allocation Hardening

### Current v0.1 goal

- The Scheduler selects a queued Job.
- It filters eligible Workers.
- It scores Workers.
- It allocates resources.
- It creates Reservations and Assignments.

### Deferred

- Concurrent Scheduler instances.
- Race conditions between Schedulers.
- Stronger locking strategy.
- Reservation expiration.
- Reservation reconciliation.
- Resource leak detection.
- Fragmentation optimization beyond the initial scoring model.

### Revisit during

- Phase 6 Scheduler implementation.
- Phase 7 Reliability hardening.

## 11. API Idempotency

### Deferred

- Idempotency keys for Job creation.
- Idempotent Worker registration.
- Idempotent cancellation.
- Idempotent Assignment and status operations.
- Duplicate request handling after client or network retries.

### Revisit in

Phase 7 — Reliability.

## 12. Attempt Retry Semantics

### Current v0.1

Jobs contain `max_retries`.

### Deferred

- Exact retry policy.
- Retryable versus non-retryable failures.
- Attempt numbering semantics.
- Backoff.
- Requeue behavior.
- Resource release before retry.
- Worker failure versus application failure classification.

### Revisit in

Phase 7 — Reliability.

## 13. Assignment and Reservation Recovery

### Deferred

- Detecting stale Assignments.
- Detecting Reservations that outlive their Assignments.
- Automatic resource release.
- Reconciliation between Job, Attempt, Assignment, and Reservation state.
- Recovery after process, database, Scheduler, or Worker failures.

### Revisit in

Phase 7 — Reliability.

## 14. Authentication and Authorization

Authentication and authorization are not part of the current v0.1 execution
path.

### Deferred

- API authentication.
- Worker authentication.
- Authorization and RBAC.
- Secure Worker-to-Control-Plane communication.
- Credential and token management.

### Revisit when

The core v0.1 execution lifecycle is working.

## 15. Observability

### Deferred

- Structured logging.
- Metrics.
- Distributed tracing.
- Scheduler metrics.
- Worker metrics.
- Job and Attempt execution metrics.
- Redis and PostgreSQL operational metrics.
- Alerting.

These can be added after the core lifecycle is operational.

## 16. Production Hardening

The initial Forge v0.1 implementation intentionally prioritizes learning the
architecture and completing the first end-to-end lifecycle.

### Deferred production-hardening concerns

- Advanced retry policies.
- Backpressure.
- Rate limiting.
- Connection pool tuning.
- Horizontal scaling.
- High availability.
- Disaster recovery.
- Backup and restore procedures.
- Security hardening.
- Performance and load testing.
- Chaos and failure testing.

These should not block the first complete Forge lifecycle unless they expose a
correctness issue.

## Important Design Principle

Deferred functionality is intentional, not forgotten.

When implementing an early phase, prefer a simple implementation that is
correct for the currently implemented architecture. Record functionality that
depends on later components under this section rather than prematurely
implementing it.

When a deferred item becomes relevant, move it into the appropriate
implementation phase and update this section to reflect its completed status.

Do not introduce future-phase complexity into the current phase unless it is
required for correctness of the current v0.1 milestone.