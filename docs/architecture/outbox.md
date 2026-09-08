# Transactional Outbox

## Purpose

Forge uses the Outbox Pattern to avoid an unsafe dual write between PostgreSQL
and Redis. A business operation such as Job creation needs durable business
state and a scheduling event. Writing the Job to PostgreSQL and publishing to
Redis independently could leave the system with a committed Job but no event if
Redis is unavailable.

The Outbox Pattern first stores both the business record and an event record in
PostgreSQL. Redis publication is then handled by `OutboxPublisher` after the
PostgreSQL transaction is complete.

```text
Business operation
    |
    +--> PostgreSQL Job
    |
    +--> PostgreSQL OutboxEvent
    |       (same transaction)
    v
OutboxPublisher
    |
    v
Redis Stream: forge:scheduling
    |
    v
Future Scheduler
```

The Scheduler in this diagram is not implemented in Phase 3.

## OutboxEvent Model

`OutboxEvent` is a PostgreSQL record with these relevant fields:

| Field | Purpose |
| --- | --- |
| `id` | UUID primary key for the outbox record. |
| `event_type` | Event name, such as `JOB_CREATED`. |
| `payload` | JSONB data needed to reconstruct the scheduling event. |
| `created_at` | Time the event record was created. |
| `published_at` | `NULL` until successful Redis publication. |
| `retry_count` | Number of recorded publish failures. Starts at `0`. |
| `last_error` | Most recent publish failure message, or `NULL`. |

Unpublished records are indexed by `created_at`, which supports selecting them
in creation order.

## Transactional Insertion

`add_outbox_event()` accepts the caller's `AsyncSession`, adds an
`OutboxEvent`, and returns it. It does not commit the session and does not
contact Redis.

A caller adds a Job and calls `add_outbox_event()` on the same session, then
commits once. PostgreSQL commits the Job and its OutboxEvent together, or
commits neither.

```text
BEGIN
  INSERT Job
  INSERT OutboxEvent
COMMIT
```

If the transaction rolls back, both the business record and the OutboxEvent are
removed. There is consequently no durable event for a business record that was
not committed.

`scheduling_event_payload()` converts a Phase 2 `SchedulingEvent` into the
minimal Redis-compatible payload:

```json
{
  "event_id": "uuid",
  "event_type": "JOB_CREATED",
  "job_id": "uuid",
  "created_at": "timestamp"
}
```

UUIDs are stored as strings and the timestamp is an ISO 8601 string. The full
Job record is not copied into the payload.

## Publishing Pending Events

`OutboxPublisher.get_pending_events()` queries for OutboxEvent records whose
`published_at` is `NULL`, ordered by `created_at` ascending.

For each selected record, `OutboxPublisher` converts the JSON payload back into
a Phase 2 `SchedulingEvent`: UUID strings become `UUID` values and the ISO
8601 timestamp becomes a timezone-aware `datetime` when the stored value
contains an offset.

The publisher then calls `RedisStreams.publish()`, which writes the event to
the configured Redis Stream using `XADD`. After a successful publish, the
publisher sets `published_at` to the current UTC time. Its session commit makes
that state durable, so future pending-event queries exclude the record.

## Failure State

If publishing an individual event raises an exception, the publisher records
the failure on that OutboxEvent:

- `published_at` remains `NULL`;
- `retry_count` increases by one; and
- `last_error` receives the exception message.

The publisher continues processing later pending events in the same explicit
publish pass. `retry_count` is state tracking only in Phase 3. There is no
automatic retry loop, exponential backoff, claiming, or recovery policy
implemented here.

## PostgreSQL and Redis Responsibilities

PostgreSQL remains the source of truth for Jobs, OutboxEvents, and all other
durable business state. Redis Streams is the event delivery mechanism: it
carries a small scheduling event to downstream consumers, not a full Job
record or authoritative scheduling state.

## Phase Boundaries

Phase 2 established the Redis Streams primitives: connection, `XADD`, consumer
groups, `XREADGROUP`, acknowledgements, and pending-message inspection. It did
not create durable event records or connect business transactions to Redis.

Phase 3 adds transactional OutboxEvent insertion and an explicit
`OutboxPublisher.publish_pending()` pass that publishes pending records and
persists publication or failure state.

Phase 7 is the future reliability phase. Retry loops, backoff, claiming,
recovery policy, stale-event handling, and broader failure recovery belong
there rather than in the current Outbox implementation.

Phase 3 does not implement the Scheduler, Workers, FastAPI APIs, resource
allocation, reservations, assignments, or automatic retry/recovery policies.
