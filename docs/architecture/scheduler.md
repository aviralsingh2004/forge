# Scheduler Architecture

## Purpose

The Scheduler is Forge's decision-making engine. It is responsible for selecting pending work, discovering healthy execution nodes, evaluating candidate node capacity and capabilities, and deterministically assigning jobs to the optimal worker.

```text
               PostgreSQL (Source of Truth)
                    │               ▲
          1. Query  │               │ 2. Query
          QUEUED    │               │ READY
          Jobs      ▼               │ Workers & Reservations
                ┌───────────────────────┐
                │       Scheduler       │
                │                       │
                │  - Job Selection      │
                │  - Worker Discovery   │
                │  - Available Resources│
                │  - Worker Filtering   │
                │  - Best-Fit Scoring   │
                │  - Worker Selection   │
                └───────────┬───────────┘
                            │
                            │ 3. Selected Candidate
                            ▼
                    (Assignment Pipeline)
```

---

## Core Principles

1. **PostgreSQL as Source of Truth**:
   Authoritative state for Jobs, Workers, Reservations, and Attempts resides exclusively in PostgreSQL. The Scheduler evaluates live database state rather than maintaining independent, potentially stale in-memory allocations.

2. **Push-Based Scheduling**:
   The Scheduler determines which worker executes each job and manages resource allocations. Workers never select their own jobs or self-allocate resources.

3. **Deterministic Selection**:
   All selection phases—from job queue prioritization to best-fit worker scoring and tie-breaking—follow strict deterministic rules.

---

## Scheduling Pipeline

The scheduling process is divided into five cohesive stages:

### 1. Deterministic Job Selection (`get_next_job`)

`get_next_job()` retrieves the next eligible job from PostgreSQL:

- **Status Filter**: Only jobs with `JobStatus.QUEUED` are eligible. Jobs in `ASSIGNED`, `RUNNING`, `SUCCEEDED`, `FAILED`, or `CANCELLED` states are excluded.
- **Priority & FIFO Ordering**:
  ```sql
  SELECT * FROM jobs
  WHERE status = 'QUEUED'
  ORDER BY priority DESC, created_at ASC
  LIMIT 1;
  ```
  - Higher-priority jobs are always scheduled before lower-priority jobs.
  - For jobs with identical priority, older jobs (`created_at ASC`) are scheduled first.
- **Return Value**: Returns a single `Job` instance, or `None` if no queued jobs exist.

### 2. Ready Worker Discovery (`get_ready_workers`)

`get_ready_workers()` queries PostgreSQL for candidate execution nodes:

- **Status Filter**: Selects workers where `Worker.status == WorkerStatus.READY`.
- **Exclusions**: Workers in `REGISTERING`, `BUSY`, `DRAINING`, or `OFFLINE` states are excluded.
- **Read-Only**: Discovery does not mutate worker states or hold exclusive locks.

### 3. Reservation-Aware Resource Calculation (`get_available_resources`)

`get_available_resources(worker)` calculates the node's true unreserved capacity:

$$\text{Available CPU} = \text{Worker CPU Capacity} - \sum_{\text{ACTIVE}} \text{Reserved CPU}$$
$$\text{Available Memory} = \text{Worker Memory Capacity} - \sum_{\text{ACTIVE}} \text{Reserved Memory}$$
$$\text{Available GPU} = \text{Worker GPU Capacity} - \sum_{\text{ACTIVE}} \text{Reserved GPU}$$

- **`ACTIVE` Reservations**: Only reservations with `ReservationStatus.ACTIVE` reduce available resources.
- **`RELEASED` Reservations**: Completed or released reservations are ignored and do not reduce available capacity.
- **Result**: Returns a tuple `(available_cpu, available_memory_mb, available_gpu)`.

### 4. Worker Filtering (`filter_workers`)

`filter_workers(job, workers)` filters candidate workers against job requirements:

A worker is eligible if and only if all of the following criteria are met:
1. **Available CPU**: $\text{Available CPU} \ge \text{job.cpu\_required}$
2. **Available Memory**: $\text{Available Memory MB} \ge \text{job.memory\_required\_mb}$
3. **Available GPU**: $\text{Available GPU} \ge \text{job.gpu\_required}$
4. **Capability Matching**: $\text{job.required\_capabilities} \subseteq \text{worker.capabilities}$

If a worker possesses sufficient raw hardware capacity but active reservations leave insufficient available resources, the worker is rejected.

### 5. Worker Scoring & Best-Fit Selection (`score_worker`, `select_worker`)

#### Scoring Formula

`score_worker(job, worker)` evaluates candidate fit by calculating remaining resource slack:

$$\text{Score} = (\text{Available CPU} - \text{job.cpu\_required},\, \text{Available Memory} - \text{job.memory\_required\_mb},\, \text{Available GPU} - \text{job.gpu\_required})$$

- Exact resource matches yield a score of `(0, 0, 0)`.
- Scores are calculated strictly from available resources remaining after active reservations.

#### Best-Fit Selection

`select_worker(job, workers)` chooses the optimal worker:

- **Empty Candidate Pool**: Returns `None` if no workers pass filtering.
- **Minimal Slack (Best-Fit)**: Selects $\min(\text{workers}, \text{key}=\text{score\_worker})$.
- **Lexicographical Tuple Ordering**:
  1. Primary: Smallest CPU slack (tightest CPU fit).
  2. Secondary: Smallest memory slack.
  3. Tertiary: Smallest GPU slack.
- **Deterministic Tie-Breaking**: In the event of identical scores, Python's `min()` preserves the first encountered worker in input order.

---

## Phase Boundaries & Deferred Concerns

- **Phase 6 (Current)** implements deterministic job selection (`get_next_job`), ready worker discovery (`get_ready_workers`), reservation-aware capacity calculation (`get_available_resources`), constraint filtering (`filter_workers`), and best-fit worker selection (`score_worker`, `select_worker`).
- **Deferred Concerns**:
  - Atomic reservation creation and database transaction locking (`SELECT FOR UPDATE`).
  - Redis Streams consumer loop integration (`forge:scheduling` stream).
  - HTTP push delivery of assignments to worker nodes (`POST /assignments`).
  - Phase 7 failure recovery (lease expiration, stale reservation reaping, worker heartbeat timeouts).

