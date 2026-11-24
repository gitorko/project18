# Task Processor Design

This document describes the behavior implemented by the backend library. It
covers task submission, PostgreSQL-backed queueing, priority and type fairness,
worker execution, retries, recovery, runtime controls, and monitoring. The
project has no UI requirement; the supported interface is HTTP plus registered
Java `TaskHandler` implementations.

## Goals and current scope

- Accept individual or bulk task requests and persist them before returning.
- Run registered task handlers asynchronously on one or more application
  instances sharing PostgreSQL.
- Bound work claimed by a worker to its configured execution capacity.
- Select higher-priority work first and share capacity across task types within
  a priority level.
- Retry tasks when a caller requests more than one attempt; recover work after a
  worker failure.
- Expose task state, queue statistics, worker controls, and Micrometer metrics.

The implementation uses PostgreSQL as both durable queue and coordination
store. It does not depend on a separate broker. The API accepts JSON task
payloads and routes by `taskType`; a task handler owns interpretation of its
payload.

## Main components

| Component | Responsibility |
| --- | --- |
| `TaskController` | Task creation, bulk creation, lookup, pagination, summary, cancellation, and this worker's processing list. |
| `PollerController` | Runtime poller configuration and status, plus start and stop endpoints. |
| `TaskQueueService` | Persist, select, claim, finalize, retry, cancel, and summarize tasks. |
| `TaskPollerService` | Poll for work, submit tasks to the executor, maintain heartbeats, and enforce task timeouts. |
| `TaskProcessorService` | Resolve a `TaskHandler`, execute it, and report success or failure. |
| `StaleTaskRecoveryService` | Reset stale in-flight work for retry or mark it failed after attempts are exhausted. |
| `TaskRepository` | JPA and PostgreSQL queue operations, including `FOR UPDATE SKIP LOCKED`. |
| `TaskMetrics` | Micrometer counters, timers, gauges, and process-local runtime/rate samples. |

## Task lifecycle

The normal lifecycle is:

```text
POST /api/v1/tasks
        |
        v
     PENDING --claim--> PROCESSING --success--> COMPLETED
                            |  \
                            |   \ attempts exhausted
                            |    --------------------> FAILED
                            |
                            +-- failure with attempts left --> PENDING
                            +-- stale worker with attempts left --> PENDING
```

`attemptCount` is incremented when a worker claims a task. A normal failure or
timeout returns the task to `PENDING` while `attemptCount < maxAttempts`; when
the current attempt reaches `maxAttempts`, it becomes `FAILED`. Stale recovery
does not increment the count a second time. Each retry is therefore another
claim/attempt.

Task finalization locks the task row and checks both its `PROCESSING` state and
attempt number. This prevents a late completion or failure from an older attempt
overwriting the state of a retry that has already started.

Processing is **at least once** across failures: a worker can perform side
effects and then die before recording completion, after which the task may run
again. Handlers should be safe to retry or use an idempotency mechanism for
external side effects.

## HTTP API

### Task endpoints

| Endpoint | Behavior |
| --- | --- |
| `POST /api/v1/tasks` | Persist one task and return `201 Created`. |
| `POST /api/v1/tasks/bulk` | Persist up to 1,000 tasks and return `201 Created`. |
| `GET /api/v1/tasks/{taskId}` | Return one task or `404`. |
| `GET /api/v1/tasks?status=...&page=...&size=...` | Return paginated tasks; page size is capped at 100. |
| `GET /api/v1/tasks/summary` | Return lifecycle totals, per-type counts/runtime, throughput, pool information, and an approximate backlog estimate. |
| `GET /api/v1/tasks/processing` | Return tasks assigned to this worker ID. |
| `DELETE /api/v1/tasks/{taskId}` | Cancel a `PENDING` or `SCHEDULED` task. It does not interrupt a running handler. |

Validation bounds are defined by the request DTOs: task type is required;
priority must be `LOW`, `MEDIUM`, or `HIGH` (default `MEDIUM`);
`maxAttempts` is from 1 to 10 when supplied; timeout is at least one second;
and a bulk request has at most 1,000 tasks.
Validation errors and illegal arguments return `400`; unexpected errors return
a generic `500` response.

### Worker control endpoints

| Endpoint | Behavior |
| --- | --- |
| `GET /api/v1/poller/config` | Return pool core size, batch size, current poll interval, enabled state, active threads, heartbeat settings, and worker ID. |
| `PUT /api/v1/poller/config` | Update pool size, batch size, poll interval, or enabled state for this process. |
| `GET /api/v1/poller/status` | Return process-local poller and executor state, including executor queue size and max pool size. |
| `POST /api/v1/poller/start` | Start polling if the poller is stopped. |
| `POST /api/v1/poller/stop` | Stop polling and wait for in-flight tasks before shutting down the executor. |

The runtime configuration API affects only the instance receiving the request.
Configure each pod separately when hardware capacity differs.

## Submission and persistence

Task data is stored in the `tasks` table and managed by Liquibase. A task has a
UUID, type, text payload, priority, attempt limits, timeout, status, worker ID,
timestamps, and the most recent error message. The schema has indexes for queue
selection, status, worker ID, and heartbeat lookups.

An omitted priority is `MEDIUM`. The API uses named priority values, while the
database stores an integer rank so SQL can efficiently sort tasks. A migration
maps existing negative priorities to `LOW`, zero to `MEDIUM`, and positive
priorities to `HIGH`. An omitted `maxAttempts` is **one**, meaning no retry
after the first failure. Set `maxAttempts` greater than one to opt into
immediate retries. An omitted timeout uses
`task-processor.poller.default-task-timeout-seconds` (120 seconds by default).

There is no queue-depth admission limit: enqueue writes to PostgreSQL without
waiting for a worker or rejecting based on backlog. Bulk request size is
limited, but total persisted backlog is not.

### Scheduled-at field status

The request and entity support `scheduledAt`, and future timestamps are stored
with status `SCHEDULED`. The current worker query only claims `PENDING` rows,
and there is no service that promotes due `SCHEDULED` rows to `PENDING`. As a
result, delayed tasks are represented in the API but are not currently executed
automatically. This is not part of the processor's immediate-task flow.

## Distributed dequeue and priority

Every enabled worker runs a poller. A poll uses a PostgreSQL transaction to
select candidate rows with:

```sql
SELECT * FROM tasks
WHERE status = 'PENDING'
  AND scheduled_at <= :now
ORDER BY priority DESC, created_at ASC
LIMIT :candidatePoolSize
FOR UPDATE SKIP LOCKED
```

`SKIP LOCKED` lets concurrent workers claim disjoint rows without a separate
leader or distributed lock. The lock is held only while the claim transaction
marks selected tasks `PROCESSING`, sets `workerId`, `startedAt` and
`lastHeartbeat`, and increments `attemptCount`. It is not held while the
handler runs.

The candidate pool size is `batchSize * fairnessCandidateMultiplier`, capped
by `maxFairnessCandidates` (defaults: batch size 10, multiplier 5, cap 200).
The queue service then applies these rules:

1. **Priority is strict.** The highest candidate priority tier is selected
   first. Lower tiers are considered only if the higher tier's candidates are
   exhausted and the batch still has free slots.
2. **Types round-robin within a tier.** Within one priority tier, the service
   takes one task per task type per pass. Tasks within each type remain in
   creation-time order.
3. **Unselected candidates stay pending.** Their row locks are released when
   the transaction commits.

This provides type fairness inside the fetched candidate window. A sustained
stream of higher-priority tasks can starve lower priorities. A type whose tasks
fall beyond the candidate cap cannot participate in that poll's round-robin
selection.

## Polling and queue latency

The poller runs one cycle at a time on a single-thread scheduled executor. It
schedules the next cycle after the current one finishes, using the latest
`currentPollInterval` value. With adaptive polling enabled:

- An empty queue doubles the interval, up to `max-poll-interval-ms` (default
  5,000 ms).
- Finding work resets it to `min-poll-interval-ms` (default 100 ms).
- The initial interval is `poll-interval-ms` (default 1,000 ms).
- `PUT /api/v1/poller/config` can change the current interval at runtime.

If adaptive polling is disabled, empty polls do not change the configured
interval. A new interval takes effect after the current cycle completes.

## Execution capacity and hardware sizing

Each process owns one `ThreadPoolExecutor` with a bounded 1,000-item executor
queue. Polling calculates available claim capacity as:

```text
corePoolSize - activeThreadCount - executorQueueSize
```

The poller claims at most that capacity and `batchSize`, avoiding intentional
executor backlog. The runtime `poolSize` setting changes the executor's core
size; `maxPoolSize` is configured as its maximum.

**Current limitation:** the poller limits claims to available core capacity, so
normal polling does not scale the executor above the core size. Also, the
executor's bounded queue normally accepts submissions before `ThreadPoolExecutor`
grows beyond its core size. In practice, set `poolSize` to the concurrency you
want; `maxPoolSize` does not currently provide automatic scale-up under normal
polling.

Capacity is per instance, not coordinated across the cluster. For CPU-bound
handlers, configure a small pool appropriate to the cores available on that
worker. I/O-bound handlers may benefit from more concurrent workers. All task
types currently share the same pool, so a long-running handler can occupy a slot
that a short task could otherwise use. There is no per-type pool, CPU/I/O
classification, cluster-wide rate limit, or automatic CPU-based resizing.

## Handler routing

`TaskProcessorService` registers all Spring `TaskHandler` beans by the value of
`getTaskType()`. A matching task type runs through that handler. If no specific
handler matches, the registered `DefaultTaskHandler` is used when available;
without a default handler, the task fails. Handlers receive the persisted `Task`
including its JSON text payload.

Handlers should use interruptible I/O and honor thread interruption. The
timeout mechanism cannot forcibly terminate Java code.

## Retries, timeout, and recovery

### Handler failure and timeout

When a handler throws, `TaskProcessorService` calls `failTask`. With attempts
remaining, the task is returned to `PENDING` immediately. Otherwise it becomes
`FAILED`. The error message stores the latest failure; the system does not keep
a separate per-attempt history or stack trace table.

The poller's heartbeat pass also checks each in-memory task against its
effective timeout. On timeout, it removes the task's in-memory tracking entry,
requests cancellation with `Future.cancel(true)`, and sends the failure through
the same attempt-guarded finalization path.

`cancel(true)` only requests interruption. A handler blocked in non-interruptible
code can continue occupying its thread after the database task has been retried
or failed. Use client-side I/O timeouts and interruption-aware handlers.

### Worker crash recovery

Every `heartbeat.interval-ms` (default 5,000 ms), the worker updates
`last_heartbeat` for each task tracked in memory. A separate scheduled recovery
service runs every `heartbeat.recovery-interval-ms` (default 10,000 ms):

- A `PROCESSING` task older than `heartbeat.stale-threshold-ms` (default
  30,000 ms) with attempts remaining is reset to `PENDING`.
- A stale task at its attempt limit is marked `FAILED`.

Recovery does not increment attempts; the next successful claim increments it.
If a worker is paused long enough to miss the stale threshold, its task may be
reclaimed and executed elsewhere. Handlers must therefore tolerate duplicate
side effects.

## Monitoring and statistics

`GET /api/v1/tasks/summary` reports total tasks by status, pending counts by
type, lifecycle counts by type, average successful runtime per type, processing
rate, active threads, current pool size, and `estimatedBacklogCompletionSeconds`.

The estimated drain time is:

```text
(pending tasks + processing tasks) * process-local average runtime
---------------------------------------------------------------
                    configured core pool size
```

It is `null` until this process has successful runtime samples. It excludes
scheduled tasks and does not account for future submissions, priority tiers,
task-type-specific runtime differences, or remaining runtime of current tasks.
The runtime samples and processing-rate window are process-local; task counts
come from the database. Consequently, the summary is not a fully cluster-wide
capacity or ETA estimate.

Spring Actuator exposes health, metrics, and Prometheus endpoints. Micrometer
metrics include enqueue/completion/failure/poll counters, per-type counters,
processing-duration timers (including p50/p95/p99 percentiles for the aggregate
timer), per-type processing timers, active-processing count, executor
pool/active/queue gauges, completion rate, and average successful runtime.
`tasks.failed` counts terminal handler/timeout failures, not each intermediate
retry failure. Stale tasks marked failed by the bulk recovery query are reflected
in database-backed summaries but do not currently increment the Micrometer
failure counters.

## Runtime configuration

Defaults are in `TaskProcessorProperties` and `application.yml`:

| Setting | Default | Purpose |
| --- | ---: | --- |
| `worker-id` | Random UUID | Identifies this process in claimed tasks. Set explicitly per pod for easier operations. |
| `poller.enabled` | `true` | Start polling on application startup. |
| `poller.pool-size` | 5 | Executor core concurrency. |
| `poller.max-pool-size` | 20 | Executor maximum; not reached by the current poll capacity policy. |
| `poller.batch-size` | 10 | Upper bound on tasks claimed per poll. |
| `poller.poll-interval-ms` | 1,000 ms | Initial poll interval. |
| `poller.min-poll-interval-ms` | 100 ms | Interval used after work is found when adaptive polling is enabled. |
| `poller.max-poll-interval-ms` | 5,000 ms | Empty-queue backoff limit. |
| `poller.adaptive-polling` | `true` | Enable empty-queue backoff and busy-queue reset. |
| `poller.fairness-candidate-multiplier` | 5 | Candidate over-fetch multiplier for type fairness. |
| `poller.max-fairness-candidates` | 200 | Candidate query cap. |
| `poller.default-task-timeout-seconds` | 120 s | Fallback task timeout. |
| `heartbeat.interval-ms` | 5,000 ms | Worker heartbeat period. |
| `heartbeat.stale-threshold-ms` | 30,000 ms | Age at which processing work is considered stale. |
| `heartbeat.recovery-interval-ms` | 10,000 ms | Stale recovery period. |

Runtime endpoint validation limits pool size and batch size to 1–100 and poll
interval to 100–60,000 ms.

## Operational limitations

- PostgreSQL is the only queue storage implementation.
- The task table has no built-in retention or cleanup policy; completed and
  failed rows continue contributing to storage and summary totals.
- There is no idempotency key or deduplication check for enqueue requests.
- The REST API has no built-in authentication or authorization layer.
- Retry is opt-in, immediate, and stores only the latest error message.
- Scheduled tasks are recorded but not promoted to runnable state.
- A running task cannot be cancelled through the task API; timeout handling is
  best-effort interruption.
- Priority is strict, so lower-priority work can starve under sustained higher
  priority load.
- Fairness is limited to task types present in each bounded candidate pool.
- Worker metrics and runtime averages are process-local; there is no shared
  worker registry or cross-node CPU/memory view.
- `POST /api/v1/poller/stop` shuts down the executor. Although a start endpoint
  exists, that executor cannot accept work again after shutdown; restart the
  application process to resume processing.
- Queueing/retry provides at-least-once execution, not exactly-once external
  side effects.
