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

## Phase 0 — Repository

Create:

```text
forge/
├── backend/
├── frontend/
├── deploy/
├── docs/
├── docker-compose.yml
├── .env.example
├── README.md
└── Makefile
```

Set up:

- Python
- dependency management
- Ruff
- pytest
- pre-commit
- Docker
- Docker Compose

---

## Phase 1 — PostgreSQL

Implement:

- SQLAlchemy models
- Alembic
- migrations
- constraints
- indexes
- database connection management

Verify CRUD for initial entities.

---

## Phase 2 — Redis Streams

Implement:

- Redis connection
- stream creation
- consumer group
- event publishing
- event consumption
- acknowledgement
- pending-message handling

---

## Phase 3 — Outbox

Implement:

- OutboxEvent model
- transactional Job + Outbox insertion
- Outbox Publisher
- retry behavior

---

## Phase 4 — FastAPI

Implement:

```text
POST /api/v1/jobs
GET /api/v1/jobs/{id}
POST /api/v1/jobs/{id}/cancel

POST /api/v1/workers/register
POST /api/v1/workers/{id}/heartbeat

POST /api/v1/workers/{id}/assignments

POST /api/v1/attempts/{id}/status

GET /health/live
GET /health/ready
```

---

## Phase 5 — Worker

Implement:

```text
registration
heartbeat loop
assignment handling
Docker executor
status reporting
```

---

## Phase 6 — Scheduler

Implement:

```text
Redis consumer
Job selection
Worker filtering
Worker scoring
resource allocation
reservation
assignment
```

---

## Phase 7 — Reliability

Implement:

- idempotency
- retries
- Redis failure recovery
- Scheduler failure recovery
- Worker failure detection
- stale assignments
- resource release

---

## Phase 8 — Nginx

Put Nginx in front of FastAPI.

Verify:

```text
localhost/api/v1/...
```

works through Nginx.

---

## Phase 9 — End-to-End

The following must work:

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