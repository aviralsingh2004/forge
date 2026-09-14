# Forge Phase 4 HTTP API

This document describes the currently implemented Forge Phase 4 FastAPI
contract. It reflects the routes, Pydantic schemas, and behavior currently in
the repository. Authentication, authorization, idempotency, rate limiting,
and broader lifecycle behavior are deferred to later phases.

Unless stated otherwise, FastAPI validation errors use HTTP `422` with its
standard validation response body. UUID path parameters and UUID request fields
must be valid UUID values.

## Common Response Models

### Job response

Job responses contain:

- `id`: UUID
- `name`: string
- `image`: string
- `command`: array of strings
- `priority`: integer
- `cpu_required`: integer
- `memory_required_mb`: integer
- `gpu_required`: integer
- `required_capabilities`: array of strings
- `status`: string
- `max_retries`: integer
- `created_at`: timestamp
- `updated_at`: timestamp

### Worker response

Worker responses contain:

- `id`: UUID
- `name`: string
- `status`: `REGISTERING`, `READY`, `BUSY`, `DRAINING`, or `OFFLINE`
- `version`: string
- `cpu_capacity`: integer
- `memory_capacity_mb`: integer
- `gpu_capacity`: integer
- `capabilities`: array of strings
- `last_heartbeat_at`: timestamp or `null`
- `created_at`: timestamp
- `updated_at`: timestamp

## Jobs

### Create a Job

`POST /api/v1/jobs`

Creates a Job in PostgreSQL and creates a corresponding scheduling OutboxEvent
in the same transaction. The endpoint does not publish directly to Redis.

#### Request body

```json
{
  "name": "image-processing",
  "image": "python:3.12",
  "command": ["python", "process.py"],
  "priority": 80,
  "cpu_required": 2,
  "memory_required_mb": 2048,
  "gpu_required": 0,
  "required_capabilities": ["python"],
  "max_retries": 3
}
```

Fields:

- `name`: required string.
- `image`: required string.
- `command`: required array of strings.
- `priority`: optional integer, default `0`, constrained to `0 <= priority <= 100`.
- `cpu_required`: required integer, constrained to `>= 0`.
- `memory_required_mb`: required integer, constrained to `>= 0`.
- `gpu_required`: required integer, constrained to `>= 0`.
- `required_capabilities`: optional array of strings, default `[]`.
- `max_retries`: optional integer, default `0`, constrained to `>= 0`.

The current `JobCreate` schema does not enforce non-empty values for `name` or
`image`.

#### Success

- `201 Created`
- Response: a Job response.
- Initial Job status: `QUEUED`.

#### Errors

- `422 Unprocessable Entity`: request validation failure.

### Get a Job

`GET /api/v1/jobs/{job_id}`

Returns one Job by UUID. This operation is read-only.

#### Success

- `200 OK`
- Response: a Job response.

#### Errors

- `404 Not Found` when the Job does not exist:

```json
{"detail": "Job not found"}
```

- `422 Unprocessable Entity` for an invalid UUID path value.

### Update a Job

`PATCH /api/v1/jobs/{job_id}`

Partially updates the supplied Job fields. The updated fields are recorded in
a `JOB_UPDATED` JobEvent, and a `JOB_UPDATED` OutboxEvent is created in the
same database transaction. The endpoint does not publish directly to Redis.

#### Request body

All fields are optional in the `JobUpdate` schema. At least one field must be
supplied for the endpoint to proceed.

- `name`: optional string, minimum length `1` when provided.
- `image`: optional string, minimum length `1` when provided.
- `command`: optional array of strings.
- `priority`: optional integer, constrained to `0 <= priority <= 100`.
- `cpu_required`: optional integer, constrained to `>= 0`.
- `memory_required_mb`: optional integer, constrained to `>= 0`.
- `gpu_required`: optional integer, constrained to `>= 0`.
- `required_capabilities`: optional array of strings.
- `max_retries`: optional integer, constrained to `>= 0`.

#### Success

- `200 OK`
- Response: the updated Job response.

#### Errors

- `400 Bad Request` when the request body contains no fields:

```json
{"detail": "No fields to update"}
```

- `404 Not Found` when the Job does not exist:

```json
{"detail": "Job not found"}
```

- `422 Unprocessable Entity` for validation failure.

### List Jobs

`GET /api/v1/jobs`

Returns all Jobs. This operation is read-only.

#### Success

- `200 OK`
- Response: an array of Job responses.
- Ordering: descending `priority`; for equal priorities, ascending
  `created_at` (older Jobs first).

There are no implemented pagination or filtering parameters.

### Cancel a Job

`POST /api/v1/jobs/{job_id}/cancel`

Cancels a queued Job. The endpoint creates a `JOB_CANCELLED` JobEvent and a
corresponding OutboxEvent as part of the database transaction. It does not
publish directly to Redis.

#### Request body

No request body.

#### Success

- `200 OK`
- Response: the Job response with status `CANCELLED`.

#### Errors

- `404 Not Found` when the Job does not exist:

```json
{"detail": "Job not found"}
```

- `409 Conflict` when the Job is not `QUEUED`:

```json
{"detail": "Only queued jobs can be cancelled"}
```

- `422 Unprocessable Entity` for an invalid UUID path value.

Only `QUEUED -> CANCELLED` is currently supported. Cancellation of assigned or
running work is deferred until Worker and Docker lifecycle coordination exists.

## Workers

### Register a Worker

`POST /api/v1/workers/register`

Creates a Worker in PostgreSQL. A newly registered Worker starts in
`REGISTERING`; registration does not automatically make it `READY`.

#### Request body

```json
{
  "name": "worker-1",
  "version": "0.1.0",
  "cpu_capacity": 8,
  "memory_capacity_mb": 16384,
  "gpu_capacity": 1,
  "capabilities": ["docker", "python"]
}
```

Fields:

- `name`: required string, minimum length `1`.
- `version`: required string, minimum length `1`.
- `cpu_capacity`: required integer, constrained to `>= 0`.
- `memory_capacity_mb`: required integer, constrained to `>= 0`.
- `gpu_capacity`: required integer, constrained to `>= 0`.
- `capabilities`: optional array of strings, default `[]`.

#### Success

- `201 Created`
- Response: a Worker response with `status` `REGISTERING` and
  `last_heartbeat_at` set to `null`.

#### Errors

- `422 Unprocessable Entity` for validation failure.
- Worker `name` is protected by a PostgreSQL unique constraint. The current
  implementation does not translate a duplicate-name database error into a
  dedicated API error response.

### Send a Worker Heartbeat

`POST /api/v1/workers/{worker_id}/heartbeat`

Records a Worker heartbeat, updates `last_heartbeat_at`, and persists a
`WorkerHeartbeat` row. It does not change Worker lifecycle status.

#### Request body

```json
{
  "cpu_usage": 3,
  "memory_usage_mb": 2048,
  "gpu_usage": 1,
  "running_attempts": [
    "00000000-0000-0000-0000-000000000001"
  ]
}
```

Fields:

- `cpu_usage`: required integer, constrained to `>= 0`.
- `memory_usage_mb`: required integer, constrained to `>= 0`.
- `gpu_usage`: required integer, constrained to `>= 0`.
- `running_attempts`: optional array of UUIDs, default `[]`.

#### Success

- `200 OK`
- Response:

```json
{
  "worker_id": "00000000-0000-0000-0000-000000000001",
  "timestamp": "2026-09-14T12:00:00Z"
}
```

#### Errors

- `404 Not Found` when the Worker does not exist:

```json
{"detail": "Worker not found"}
```

- `422 Unprocessable Entity` for invalid usage values, missing required usage
  fields, invalid UUIDs in `running_attempts`, or an invalid Worker UUID.

The current endpoint does not validate usage against Worker capacity or perform
failure detection, readiness transitions, assignment reconciliation, or other
later Worker lifecycle behavior.

### Deliver an Assignment to a Worker

`POST /api/v1/workers/{worker_id}/assignments`

Marks an existing Assignment as delivered to the specified Worker. It does not
create an Assignment, Reservation, or Attempt, and it does not change Attempt
status.

#### Request body

```json
{
  "assignment_id": "00000000-0000-0000-0000-000000000002"
}
```

Fields:

- `assignment_id`: required UUID.

#### Success

- `200 OK`
- Response: an Assignment response containing `id`, `attempt_id`, `worker_id`,
  `status`, `created_at`, `delivered_at`, `acknowledged_at`, and
  `completed_at`.
- The Assignment status changes from `CREATED` to `DELIVERED`.
- `delivered_at` is populated.

#### Errors

- `404 Not Found` when the Worker does not exist:

```json
{"detail": "Worker not found"}
```

- `404 Not Found` when the Assignment does not exist:

```json
{"detail": "Assignment not found"}
```

- `409 Conflict` when the Assignment belongs to another Worker:

```json
{"detail": "Assignment does not belong to worker"}
```

- `409 Conflict` when the Assignment is not in `CREATED` state:

```json
{"detail": "Assignment is not in CREATED state"}
```

- `422 Unprocessable Entity` for invalid UUID path or body values.

## Attempts

### Update Attempt Status

`POST /api/v1/attempts/{attempt_id}/status`

Updates an Attempt status and any supplied result fields. The endpoint updates
Attempt state and timestamps only. It does not yet perform the later lifecycle
cascade involving Assignment, Reservation, Job, or Worker.

#### Request body

```json
{
  "status": "SUCCEEDED",
  "exit_code": 0,
  "error_message": null
}
```

Fields:

- `status`: required Attempt status enum value.
- `exit_code`: optional integer.
- `error_message`: optional string or `null`.

#### Success

- `200 OK`
- Response: an Attempt status response containing `id`, `status`,
  `started_at`, `finished_at`, `exit_code`, and `error_message`.

#### Implemented transitions

```text
ASSIGNED -> STARTING -> RUNNING -> SUCCEEDED
                         |
                         +------> FAILED
                         |
                         +------> CANCELLED
```

When transitioning to `STARTING`, `started_at` is populated only if it is
currently null. Existing `started_at` values are preserved. When transitioning
to `SUCCEEDED`, `FAILED`, or `CANCELLED`, `finished_at` is populated. Supplied
`exit_code` and `error_message` values are persisted.

#### Errors

- `404 Not Found` when the Attempt does not exist:

```json
{"detail": "Attempt not found"}
```

- `409 Conflict` for an invalid status transition. The detail identifies the
  current and requested statuses, for example:

```json
{
  "detail": "Invalid attempt status transition: RUNNING -> STARTING"
}
```

- `422 Unprocessable Entity` for invalid UUID path values or invalid request
  body values.

## Health

### Liveness

`GET /health/live`

A simple process liveness check.

#### Success

- `200 OK`
- Response:

```json
{"status": "ok"}
```

### Readiness

`GET /health/ready`

Checks PostgreSQL connectivity by executing a simple database query through the
existing async session dependency.

#### Success

- `200 OK`
- Response:

```json
{"status": "ok"}
```

#### Errors

- `503 Service Unavailable` when PostgreSQL is unavailable:

```json
{"detail": "PostgreSQL is unavailable"}
```

## Deferred Concerns

The current Phase 4 contract does not include authentication, authorization,
idempotency keys, rate limiting, pagination, scheduler behavior, resource
allocation, Docker execution, or the broader Job/Attempt/Assignment/Reservation
lifecycle. Those concerns are deferred to later phases unless required for
correctness of the currently implemented API.
