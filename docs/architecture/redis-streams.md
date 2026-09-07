# Redis Streams Foundation

## Purpose

Forge uses Redis Streams as the scheduling event mechanism. It provides the
stream, consumer-group, acknowledgement, and pending-message primitives that a
future Scheduler will consume.

Phase 2 does **not** implement the Scheduler. It only establishes the Redis
Streams infrastructure needed by that later phase.

## Architecture

```text
PostgreSQL
    |
    | future Outbox Publisher
    v
Redis Stream
forge:scheduling
    |
    v
scheduler-group
    |
    v
Future Scheduler
```

The Outbox Publisher is a future Phase 3 component, and the Scheduler belongs
to a later phase. Neither is implemented in Phase 2. Phase 2 can publish
events directly for infrastructure verification, but PostgreSQL remains the
authoritative store.

## Stream Configuration

The configuration layer reads Redis settings from environment variables:

| Setting | Default | Environment variable |
| --- | --- | --- |
| Stream name | `forge:scheduling` | `REDIS_STREAM_NAME` |
| Consumer group | `scheduler-group` | `REDIS_CONSUMER_GROUP` |

The Redis connection itself is configured with `REDIS_HOST`, `REDIS_PORT`, and
`REDIS_DB`.

## Event Structure

`SchedulingEvent` contains the minimal information needed to identify a
scheduling event:

```json
{
  "event_id": "uuid",
  "event_type": "JOB_CREATED",
  "job_id": "uuid",
  "created_at": "timestamp"
}
```

All four fields are required. `event_id` and `job_id` are UUID values written
as their standard string representation. `created_at` is an ISO 8601 datetime
string produced with `datetime.isoformat()`.

The full Job record is deliberately not stored in Redis. A future consumer can
use `job_id` to load authoritative state from PostgreSQL, which remains the
source of truth for Jobs and other durable business state.

## Serialization

Publishing writes simple Redis Stream fields with string values:

- UUIDs are converted with `str()`.
- `event_type` is already a string.
- The timestamp is converted with `isoformat()`.

When a message is read, `RedisStreams.deserialize_event()` reconstructs a
`SchedulingEvent` by parsing the UUID strings and ISO 8601 timestamp. No
separate serialization format or library is introduced.

## Redis Operations

`RedisStreams` currently implements the following asynchronous operations:

| Operation | Redis primitive | Purpose |
| --- | --- | --- |
| `connect()` | client connection | Creates the configured async Redis client. |
| `health_check()` | `PING` | Reports whether Redis responds to a ping. |
| `publish(event)` | `XADD` | Adds event fields to the configured stream and returns the stream message ID. |
| `create_consumer_group()` | `XGROUP CREATE` | Creates the configured group and stream when needed. |
| `read_group(...)` | `XREADGROUP` | Reads new messages for a named consumer without acknowledging them. |
| `acknowledge(message_id)` | `XACK` | Explicitly marks a processed group message as acknowledged. |
| `pending()` | `XPENDING` range inspection | Returns pending-message delivery information for the configured group. |
| `close()` | async client close | Closes the client/pool and clears the in-memory client reference. |

## Consumer Groups

`scheduler-group` is the configured consumer group for the future Scheduler.
A consumer name identifies one consumer within that group, such as a Scheduler
instance. Establishing consumer groups now verifies the delivery and pending
message semantics that later scheduling code will need; it does not implement
any scheduling behavior.

Calling `create_consumer_group()` when the configured group already exists is
handled gracefully. Other Redis errors are allowed to propagate.

## Acknowledgement and Pending Messages

```text
XADD
  |
  v
XREADGROUP
  |
  v
message becomes pending
  |
  +---- XACK ----> no longer pending
  |
  +---- consumer crashes ----> remains pending
```

`read_group()` never acknowledges messages automatically. The caller must
explicitly call `acknowledge()` after successful processing. Pending messages
can be inspected with `pending()`.

Automatic retry, claiming, redelivery, and recovery policy are intentionally
deferred to a later Scheduler/reliability phase.

## Idempotency

Every event has an `event_id` so future consumers can use it for idempotent
processing. Full application-level idempotency is **not** implemented in Phase
2.

## PostgreSQL Authority

PostgreSQL owns durable business state, including Jobs and the future Outbox
events. Redis Streams supplies event delivery mechanics only: publishing,
consumer groups, pending messages, and acknowledgement. Redis is therefore not
a replacement source of truth for Job or scheduling state.

## RabbitMQ

RabbitMQ is not used in v0.1. The project architecture selects Redis Streams
for the initial scheduling event mechanism and explicitly defers RabbitMQ until
there is a concrete requirement that justifies it.

## Testing

The Redis integration tests cover:

- connection and health checks;
- stream use and event publishing;
- consumer-group reads;
- event deserialization;
- consumer-group creation, including an already-existing group;
- acknowledgement and removal from the pending set; and
- unacknowledged messages appearing in the pending set.

Tests use isolated stream and group names and clean up their test stream. They
skip cleanly when Redis is unavailable.

## Phase Boundary

Phase 2 implements Redis configuration, async connection and health checks,
event publishing and deserialization, consumer-group creation and reads,
explicit acknowledgement, pending-message inspection, and connection closing.

Phase 2 deliberately does **not** implement an Outbox Publisher, Scheduler,
job or worker selection, resource allocation, reservations, assignments,
retries, recovery policy, FastAPI APIs, Workers, or any Phase 3+ behavior.
