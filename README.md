# Task Processor

A distributed task queue implementation using Spring Boot and PostgreSQL. Similar to JobRunr but without license restrictions.

## Features

- **Distributed Queue**: Multiple pods/nodes can process tasks without duplicates using PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED`
- **Configurable Thread Pool**: Dynamically adjust processing capacity at runtime
- **Adaptive Polling**: Reduces DB load when queue is empty, and polls early when a finished task frees a worker slot
- **Heartbeat Mechanism**: Batched heartbeats detect and recover stuck tasks from crashed workers; tasks still running at shutdown are released straight back to the queue
- **Observability**: Queue-wait, per-attempt duration (by outcome), retry, timeout, stale-recovery, release and purge metrics, plus a correlation id that follows a task from the enqueuing request into its execution logs
- **Retention**: Finished (completed, failed, cancelled) tasks are purged automatically after a configurable window
- **Retry Support**: Exponential retry delay with jitter and manual replay of failed tasks
- **Priority Queue**: Higher priority tasks are processed first
- **Scheduled Tasks**: Run once at a specified time or repeat on a fixed-delay interval
- **Metrics**: Spring Actuator metrics for monitoring processing rate and performance
- **REST API**: Full API for task management and configuration

## Quick Start

### Prerequisites

- JDK 17 or newer for local builds and runs (the Docker image uses JDK 21 to build)
- Docker with Docker Compose, or a PostgreSQL 12+ instance

### Build and test

The Gradle wrapper downloads and uses the project's pinned Gradle version, so a
separate Gradle installation is not required. From the repository root, build
the application and run the unit tests:

```bash
./gradlew clean build
```

The executable Spring Boot JAR is created under `build/libs/`. To run only the
unit tests:

```bash
./gradlew test
```

Gradle is configured in `gradle.properties` to prefer IPv4 for repository
connections, which can help on networks with unusable IPv6 routes.

If Gradle starts but reports that a plugin or dependency cannot be resolved,
check that your network or proxy allows Maven Central and the Gradle Plugin
Portal. The IPv4 option does not fix a blocked repository connection. You can
check Maven Central connectivity with:

```bash
curl -I https://repo.maven.apache.org/maven2/
```

If Gradle still reports `Broken pipe` while resolving a plugin, stop old daemons
and retry with a fresh process so it picks up the Gradle JVM settings:

```bash
./gradlew --stop
./gradlew clean build --no-daemon
```

### Run locally

1. Start PostgreSQL with Docker Compose:

```bash
docker compose up -d postgres
```

2. Start the application, pointing it at the database created by Compose:

```bash
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/taskprocessor \
SPRING_DATASOURCE_USERNAME=postgres \
SPRING_DATASOURCE_PASSWORD=postgres \
./gradlew bootRun
```

The API is available at `http://localhost:8080`. The environment variables
match the database credentials in `docker-compose.yml`; set the corresponding
Spring datasource properties if using a different PostgreSQL instance.

### Build the Docker image

The Dockerfile builds the JAR in a JDK 21 builder image and runs it on a JRE 21
image. Build the image with:

```bash
docker build -t task-processor:local .
```

To run that image, configure `SPRING_DATASOURCE_URL`,
`SPRING_DATASOURCE_USERNAME`, and `SPRING_DATASOURCE_PASSWORD` to point to your
PostgreSQL instance. When PostgreSQL runs in Docker Compose, use the Compose
service name `postgres` as the database host for a containerized app.

### Submit a sample task

The application includes a `SampleTaskHandler` for the `sample` task type. Once
the app is running, submit a task with `curl`:

```bash
curl -i -X POST http://localhost:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"taskType":"sample","payload":"{\"processingTimeMs\":100}","priority":"MEDIUM","maxAttempts":3}'
```

The response contains the task ID. Use it to check the task's status and
result; it will move from `PENDING` to `PROCESSING` and then `COMPLETED`:

```bash
TASK_ID='paste-task-id-from-response'
curl "http://localhost:8080/api/v1/tasks/${TASK_ID}"
```

The built-in handler is a demonstration. In an application, register a
`TaskHandler` for each task type and put that type's input data in `payload`.

## API Reference

### Task Management

#### Create a Task

Priority is a named value: `LOW`, `MEDIUM` (the default), or `HIGH`.
Higher-priority work is selected first. Within a priority, tasks are shared
between task types in round-robin order. A sustained stream of `HIGH` tasks can
delay `MEDIUM` and `LOW` work, so use priority levels deliberately.

```bash
POST /api/v1/tasks
Content-Type: application/json
Idempotency-Key: send-email-123

{
  "taskType": "sample",
  "payload": "{\"key\": \"value\"}",
  "priority": "HIGH",
  "maxAttempts": 3
}
```

For a one-time delayed task, include `scheduledAt` as an ISO-8601 timestamp. The
dispatcher promotes it into the runnable queue when that time arrives.

```json
{
  "taskType": "send-report",
  "payload": "{\"reportId\":\"RPT-123\"}",
  "scheduledAt": "2026-11-25T09:00:00Z"
}
```

For `POST /api/v1/tasks`, an optional `Idempotency-Key` header makes client
retries safe when a response is lost. Repeating the same key and request returns
the original task instead of enqueuing another one. Reusing a key with a
different request returns `409 Conflict`. Bulk creation does not currently
support idempotency keys.

This protects task acceptance only. It does not guarantee exactly-once execution
of handler side effects: after a worker crash, a task may run again. Handlers
that call external services should be safe to repeat or pass an idempotency key
to those services.

#### Create a Recurring Schedule

Use a recurring schedule when the same task should run repeatedly. Each run gets
its own task ID and history. The interval is a fixed delay after the previous
run finishes, so slow runs do not overlap and downtime does not create a
catch-up burst. Retries for one occurrence finish before the next interval
starts.

```bash
POST /api/v1/schedules
Content-Type: application/json

{
  "taskType": "refresh-cache",
  "payload": "{}",
  "priority": "MEDIUM",
  "scheduledAt": "2026-11-25T09:00:00Z",
  "intervalSeconds": 3600,
  "maxAttempts": 3
}
```

The response includes a schedule ID, the next run time, and the active task ID
when an occurrence is queued or running. Retrieve a schedule with
`GET /api/v1/schedules/{scheduleId}` and stop future runs with
`DELETE /api/v1/schedules/{scheduleId}`. Cancelling a schedule also cancels its
queued occurrence; an already running task is allowed to finish.

Recurring schedules use fixed-delay intervals; cron expressions and
time-zone-based calendars are not supported. `intervalSeconds` must be between
1 second and 1 year.

#### Create Tasks in Bulk

```bash
POST /api/v1/tasks/bulk
Content-Type: application/json

{
  "tasks": [
    {"taskType": "sample", "payload": "{\"id\": 1}"},
    {"taskType": "sample", "payload": "{\"id\": 2}"}
  ]
}
```

#### Get Task by ID

```bash
GET /api/v1/tasks/{taskId}
```

#### Get Tasks (Paginated)

```bash
GET /api/v1/tasks?status=PENDING&page=0&size=20
```

#### Get Task Summary

```bash
GET /api/v1/tasks/summary
```

Response:
```json
{
  "totalTasks": 1000,
  "pendingTasks": 500,
  "processingTasks": 10,
  "completedTasks": 480,
  "failedTasks": 10,
  "processingRatePerMinute": 120.5,
  "avgProcessingTimeMs": 450.2,
  "estimatedBacklogCompletionSeconds": 187,
  "activeWorkers": 5,
  "currentPoolSize": 10,
  "byTaskType": {
    "sample": {
      "pendingTasks": 490,
      "scheduledTasks": 0,
      "processingTasks": 10,
      "completedTasks": 480,
      "failedTasks": 10,
      "cancelledTasks": 0,
      "avgProcessingTimeMs": 450.2
    }
  }
}
```

#### Backpressure guidance

The summary endpoint can help a client pace submissions. Check `pendingTasks`,
per-type pending counts, and `estimatedBacklogCompletionSeconds`; if the queue
is growing or the estimated drain time is too high, pause or reduce submissions.
Using separate “slow down” and “resume” thresholds helps avoid repeatedly
switching pace as the count moves around one boundary.

The processor also enforces an outstanding-task limit. It defaults to 10,000
and counts `PENDING`, `PROCESSING`, and `SCHEDULED` task rows. Configure
`task-processor.admission.max-outstanding-tasks` or set it to `0` to disable the
global cap. Optional task-type caps are configured under
`task-processor.admission.max-outstanding-by-type`:

```yaml
task-processor:
  admission:
    max-outstanding-tasks: 20000
    max-outstanding-by-type:
      REPORT: 100
      EMAIL: 2000
```

Admission checks are serialized in PostgreSQL, so concurrent requests and
worker instances cannot pass the same stale count check. If a submission would
exceed a limit, the API returns `503 Service Unavailable` with
`Retry-After: 5`; bulk submissions are rejected as a whole. Due recurring runs
wait to enter the queue until capacity is available. Clients can use the summary
to slow down before the hard limit is reached, but counts remain a snapshot.
Queue depth is database-backed, while throughput and ETA inputs are
process-local estimates.

`estimatedBacklogCompletionSeconds` is an approximate drain time calculated
from pending plus running tasks, the average successful runtime observed by
this process, and the current worker pool size. It is `null` until the process
has completed tasks and collected runtime samples. Scheduled tasks and future
recurring occurrences are excluded until runnable; estimates do not account for
future submissions, priority tiers, or remaining runtime of active tasks.

#### Replay a Failed Task

Tasks with `FAILED` status remain available as dead-letter records until the
retention purge removes them (by default on its next run; see
`retention.retention-hours`):

```bash
GET /api/v1/tasks?status=FAILED
POST /api/v1/tasks/{taskId}/replay
```

Replay creates a new pending task linked to the failed record through
`replayOfTaskId`; the original failure remains unchanged. Only failed tasks can
be replayed.

#### Cancel a Task

```bash
DELETE /api/v1/tasks/{taskId}
```

### Poller Configuration

The shared executor can be paired with per-task-type active-task caps. For
example, to keep CPU-heavy report work from taking all the threads on each
worker, set:

```yaml
task-processor:
  poller:
    pool-size: 8
    max-concurrent-by-type:
      REPORT: 2
      EMAIL: 6
```

These are per-instance limits: with three worker instances, `REPORT: 2` allows
up to six report tasks across the deployment. Types without a configured cap
can use any available executor slot. This is workload isolation within a shared
pool; it does not create separate executors or reserve threads for a type.

#### Get Current Configuration

```bash
GET /api/v1/poller/config
```

#### Update Configuration (Scale Processing)

```bash
PUT /api/v1/poller/config
Content-Type: application/json

{
  "poolSize": 20,
  "batchSize": 50,
  "pollIntervalMs": 500,
  "enabled": true
}
```

#### Get Poller Status

```bash
GET /api/v1/poller/status
```

### Metrics

Prometheus metrics available at:
```bash
GET /actuator/prometheus
```

Key metrics:
- `tasks.enqueued` - Total tasks enqueued
- `tasks.completed` - Total tasks completed
- `tasks.failed` - Total tasks failed (including tasks failed by stale recovery)
- `tasks.processing.active` - Currently processing tasks
- `tasks.processing.duration` - Processing time histogram
- `tasks.processing.rate` - Tasks per minute
- `tasks.queue.wait.duration{type}` - Time between a task becoming runnable and a worker claiming it (p50/p95/p99); the best signal that workers are falling behind
- `tasks.attempt.duration{type,outcome}` - Handler time per attempt; `outcome` is `completed`, `retried`, `failed` or `timeout`
- `tasks.retried{type}` - Attempts that failed and were scheduled for retry
- `tasks.timeouts{type}` - Tasks cancelled for exceeding their timeout
- `tasks.stale.reset` / `tasks.stale.failed` - Tasks recovered or failed by stale-task recovery
- `tasks.released` - Claimed tasks returned to the queue (shutdown, executor rejection)
- `tasks.purged` - Finished tasks deleted by the retention purge

#### Tracing a task with a correlation id

Every request gets a correlation id: the caller's `X-Correlation-Id` header when
it is present and well formed (letters, digits, `.`, `_`, `:`, `-`, up to 100
characters), otherwise a generated one. It is echoed in the response header,
stored on any task the request creates (`correlationId` in the task response),
and put in the logging context while the task runs, together with `taskId`,
`taskType` and `attempt`. The default log pattern prints
`[correlationId] [taskId]`, so a task's execution logs can be tied back to the
request that enqueued it.

## Configuration

### Application Properties

```yaml
task-processor:
  # Unique worker ID (auto-generated if not set)
  worker-id: ${HOSTNAME:${random.uuid}}
  
  poller:
    enabled: true
    pool-size: 5           # Initial thread pool size
    max-pool-size: 20      # Maximum threads
    batch-size: 10         # Tasks per poll
    poll-interval-ms: 1000 # Base poll interval
    adaptive-polling: true # Reduce polling when queue empty
    shutdown-timeout-seconds: 30 # Wait for running tasks on shutdown before releasing them
    # Optional per-instance active-task caps by type:
    # max-concurrent-by-type:
    #   REPORT: 2
    #   EMAIL: 10

  retry:
    initial-delay-ms: 1000
    max-delay-ms: 60000
    jitter-factor: 0.2

  admission:
    max-outstanding-tasks: 10000
    max-outstanding-by-type: {}
  
  heartbeat:
    interval-ms: 5000      # Heartbeat frequency
    stale-threshold-ms: 30000  # When to consider task stuck
    recovery-interval-ms: 10000 # Stale task check frequency
    timeout-check-interval-ms: 1000 # Task execution timeout check frequency

  retention:
    enabled: true          # Purge finished (COMPLETED/FAILED/CANCELLED) tasks
    retention-hours: 0     # 0 = purge finished tasks on the next run; e.g. 168 keeps them 7 days
    purge-interval-ms: 300000
    batch-size: 500
```

Additional settings and defaults:

| Setting | Default | Purpose |
| --- | ---: | --- |
| `poller.min-poll-interval-ms` | 100 ms | Poll interval after finding work. |
| `poller.max-poll-interval-ms` | 5,000 ms | Maximum empty-queue backoff. |
| `poller.fairness-candidate-multiplier` | 5 | Candidate rows fetched per requested batch slot. |
| `poller.max-fairness-candidates` | 200 | Maximum candidate rows considered for fairness. |
| `poller.default-task-timeout-seconds` | 120 s | Timeout for tasks without an override. |
| `poller.max-concurrent-by-type` | empty | Optional per-worker active-task caps by type. |
| `scheduler.poll-interval-ms` | 1,000 ms | How often due tasks and recurring schedules are dispatched. |
| `retry.initial-delay-ms` | 1,000 ms | Initial retry backoff delay. |
| `retry.max-delay-ms` | 60,000 ms | Maximum retry backoff before jitter. |
| `retry.jitter-factor` | 0.2 | Proportional random spread added to retry delays. |
| `admission.max-outstanding-tasks` | 10,000 | Global pending, processing, and scheduled task limit; `0` disables it. |
| `admission.max-outstanding-by-type` | empty | Optional outstanding-task limits by type. |
| `heartbeat.interval-ms` | 5,000 ms | Worker heartbeat interval. |
| `heartbeat.stale-threshold-ms` | 30,000 ms | Time without heartbeat before a task is stale. |
| `heartbeat.recovery-interval-ms` | 10,000 ms | How often stale tasks are recovered. |
| `heartbeat.timeout-check-interval-ms` | 1,000 ms | How often running tasks are checked against their timeout. |
| `poller.shutdown-timeout-seconds` | 30 s | How long a stop or shutdown waits for running tasks before interrupting and releasing them. |
| `retention.enabled` | true | Purge `COMPLETED`, `FAILED` and `CANCELLED` tasks once they pass the retention window. |
| `retention.retention-hours` | 0 h | Age (since last update) after which a finished task is purged; `0` purges on the next run. |
| `retention.purge-interval-ms` | 300,000 ms | How often the purge runs. |
| `retention.batch-size` | 500 | Rows deleted per transaction; the purge repeats until a batch comes back short. |

### Environment Variables

For Kubernetes/Docker deployments:

```bash
TASK_PROCESSOR_WORKER_ID=pod-name
TASK_PROCESSOR_POLLER_POOL_SIZE=20
TASK_PROCESSOR_POLLER_BATCH_SIZE=50
SPRING_DATASOURCE_URL=jdbc:postgresql://db:5432/taskprocessor
```

## Scaling for High Throughput

When you have millions of tasks to process:

1. **Increase Thread Pool Size**:
```bash
curl -X PUT http://localhost:8080/api/v1/poller/config \
  -H "Content-Type: application/json" \
  -d '{"poolSize": 50}'
```

2. **Increase Batch Size**:
```bash
curl -X PUT http://localhost:8080/api/v1/poller/config \
  -H "Content-Type: application/json" \
  -d '{"batchSize": 100}'
```

3. **Reduce Poll Interval**:
```bash
curl -X PUT http://localhost:8080/api/v1/poller/config \
  -H "Content-Type: application/json" \
  -d '{"pollIntervalMs": 100}'
```

4. **Deploy Multiple Pods**: Each pod will independently process tasks without conflicts.

## Architecture and Runtime Behavior

This section describes how the backend components work together, including
queue selection, execution limits, retries, and recovery. PostgreSQL is both
the durable task store and the coordination mechanism; no separate message
broker is required.

### Main Components

| Component | Responsibility |
| --- | --- |
| `TaskController` | Task creation, bulk creation, lookup, pagination, summary, cancellation, replay, and this worker's processing list. |
| `TaskScheduleController` | Create, inspect, and cancel recurring schedules. |
| `PollerController` | Read and update local poller configuration and status; start or stop polling. |
| `TaskQueueService` | Persist, select, claim, finalize, retry, cancel, replay, and summarize tasks. |
| `QueueAdmissionService` | Enforce global and per-type outstanding-task limits across workers. |
| `TaskPollerService` | Poll for work, submit it to the executor, maintain heartbeats, and enforce timeouts. |
| `TaskProcessorService` | Resolve a `TaskHandler`, execute it, and report success or failure. |
| `ScheduledTaskService` | Promote due tasks and materialize recurring occurrences. |
| `StaleTaskRecoveryService` | Reset stale in-flight work or mark exhausted tasks as failed. |
| `CorrelationIdFilter` | Assign or accept a correlation id per request and expose it to logs and new tasks. |
| `TaskRetentionService` | Purge finished tasks (`COMPLETED`, `FAILED`, `CANCELLED`) older than the retention window. |
| `TaskRepository` | JPA and PostgreSQL-specific queue operations, including `FOR UPDATE SKIP LOCKED`. |
| `TaskMetrics` | Micrometer counters, timers, gauges, and process-local runtime/rate samples. |

### Task Lifecycle and Delivery

```text
POST /api/v1/tasks
        |
        v
     PENDING --claim--> PROCESSING --success--> COMPLETED
                            |  \
                            |   +-- failure with attempts left --> SCHEDULED
                            |                                      (retry delay)
                            +-- attempts exhausted -------------> FAILED

     PROCESSING --worker heartbeat expires, attempts remain-----> PENDING
```

`attemptCount` increases when a worker claims a task. A handler failure with
attempts remaining changes the task to `SCHEDULED` until the backoff delay
expires; retries are not immediate. Stale-worker recovery returns a task to
`PENDING` without incrementing the attempt count again. At the attempt limit,
the task becomes `FAILED`.

Task completion and failure lock the row and check its `PROCESSING` state and
attempt number. This prevents a late result from an older attempt overwriting a
newer attempt's state. Delivery is **at least once**: a worker can perform an
external side effect and stop before recording completion, so a task may run
again. Handlers should be safe to repeat or use idempotency with external
services.

### Additional API Endpoints and Validation

Task creation, lookup, schedules, queue summaries, and replay are described in
the API sections above. The remaining worker-control endpoints are:

| Endpoint | Behavior |
| --- | --- |
| `GET /api/v1/tasks/processing` | List tasks assigned to this worker ID. |
| `GET /api/v1/poller/config` | Return this process's pool size, batch size, poll interval, enabled state, active threads, heartbeat settings, and worker ID. |
| `PUT /api/v1/poller/config` | Update pool size, batch size, poll interval, or enabled state for this process. |
| `GET /api/v1/poller/status` | Return local poller and executor state, including active tasks and executor queue size. |
| `POST /api/v1/poller/start` | Start the poller if it is stopped. |
| `POST /api/v1/poller/stop` | Stop polling and wait for in-flight tasks to drain; the executor remains available for a later start. |

Runtime controls affect only the worker instance that receives the request.
Configure each instance separately when its hardware capacity differs.

Request validation requires a nonblank task type; `maxAttempts` from 1 to 10
when supplied; timeouts of at least one second; intervals from one second to
one year; and no more than 1,000 tasks per bulk request. Poller updates allow
pool and batch sizes from 1 to 100 and poll intervals from 100 to 60,000 ms.
Validation errors return `400 Bad Request`; an idempotency conflict or an
attempt to replay a nonfailed task returns `409 Conflict`.

### PostgreSQL Queue Claims, Priority, and Fairness

Each worker selects candidate rows in a transaction using PostgreSQL row locks:

```sql
SELECT * FROM tasks
WHERE status = 'PENDING'
  AND scheduled_at <= :now
ORDER BY priority DESC, created_at ASC
LIMIT :candidatePoolSize
FOR UPDATE SKIP LOCKED
```

`SKIP LOCKED` lets concurrent workers claim different rows without a separate
leader or distributed lock. The candidate query reads only `id`, `task_type` and
`priority`, so payloads of rows that are not claimed are never loaded. After fair
selection, the chosen rows are claimed with one
`UPDATE ... SET status = 'PROCESSING' ... RETURNING *`, which stores the worker
ID and timestamps, increments `attemptCount` and returns the full rows. The lock
is held only for that transaction and released before handlers run.

Completion and failure are single guarded `UPDATE` statements
(`WHERE status = 'PROCESSING' AND attempt_count = :attempt`), so there is no
lock-read-save round trip and a late result from an older attempt changes
nothing.

Task types that have reached their `max-concurrent-by-type` cap on this worker
are excluded in SQL (`task_type NOT IN (...)`), so a saturated type cannot fill
the candidate pool and block other types. If a type reaches its cap part-way
through a batch while the pool was full, dequeue looks deeper in the queue (up
to three refetches), skipping rows it has already considered.

Only `PENDING` rows are candidates for polling. Rows marked `FAILED` stay in the
database for inspection and are excluded from the claim query and its partial
pending-task index, even when there are many failed rows. A failed task runs
again only through the explicit replay endpoint, which creates a new pending
task.

The candidate pool is `batchSize * fairnessCandidateMultiplier`, capped by
`maxFairnessCandidates` (defaults: batch size 10, multiplier 5, cap 200). The
queue service selects the highest priority tier first, then shares that tier
across task types in round-robin order, preserving creation-time order within
each type. Lower-priority candidates are used only if higher-priority
candidates do not fill the batch. Unselected rows remain pending.

Fairness applies only to task types in the bounded candidate window. Tasks
beyond that window cannot take part in that poll's round-robin selection. A
continuous stream of higher-priority work can starve lower-priority tasks.
Priority is stored as a numeric rank in PostgreSQL for ordering, while API
clients use the `HIGH`, `MEDIUM`, and `LOW` enum names.

### Polling and Execution Capacity

The poller runs one polling cycle at a time. With adaptive polling enabled, an
empty queue doubles the interval up to `max-poll-interval-ms` (5,000 ms by
default); finding tasks resets it to `min-poll-interval-ms` (100 ms by
default). The first interval is `poll-interval-ms` (1,000 ms by default).
When adaptive polling is disabled, empty polls do not change the interval.
Runtime updates take effect after the current poll cycle completes.

When a task finishes and the previous poll found work, the poller polls again
instead of waiting for the next interval, so a freed slot does not sit idle.
This early poll is rate-limited by `min-poll-interval-ms` and is skipped while
the queue is empty, so adaptive backoff still protects the database.

Each worker has one thread pool with a bounded 1,000-task executor queue. The
poller calculates available claim capacity as:

```text
corePoolSize - activeThreadCount - executorQueueSize
```

It claims at most that capacity and the configured batch size, avoiding
intentional local executor backlog. `poolSize` changes the executor's core
size. Although `maxPoolSize` is configurable, normal polling is limited by
available core capacity, so it does not automatically scale execution above
the core size; it acts only as a ceiling for runtime resizing. In practice, set
`poolSize` to the concurrency you want. If the executor rejects a claimed task
(for example during shutdown), the task is released back to the queue instead of
being run on the polling thread.

Per-type active-task caps are enforced independently on each worker, not across
the cluster. All types share one executor; there are no separate per-type
executors, automatic CPU-based resizing, or cluster-wide resource limits. For
CPU-bound handlers, use a pool suited to the worker's cores. I/O-bound handlers
may benefit from more concurrent threads.

### Scheduled Tasks

A future one-time task is saved with `SCHEDULED` status. Distributed dispatchers
promote due rows to `PENDING` in bounded batches using `FOR UPDATE SKIP LOCKED`,
so multiple instances can safely run the dispatcher.

An interval schedule stores one task definition and creates a separate task
row for each occurrence. The next occurrence is materialized only after the
current one reaches a terminal state, including after retries. Its fixed delay
starts after that completion, preventing overlap and catch-up bursts after
downtime. Due recurring occurrences wait when admission capacity is full.
Cancellation disables future occurrences and cancels a queued occurrence; an
already running handler is allowed to finish.

Scheduled dispatch runs every second by default, configurable with
`task-processor.scheduler.poll-interval-ms`. Cron expressions and
time-zone-based calendars are not supported.

### Handler Routing, Timeouts, and Recovery

Implement `TaskHandler` and register it as a Spring bean. Tasks are routed by
`getTaskType()`. If no specific handler matches, `DefaultTaskHandler` is used
when available; otherwise the task fails. The built-in default handler is a
sample that logs the task and returns successfully, so an unregistered task
type can be marked `COMPLETED` without doing its intended work. Register a real
handler for every task type or replace the default handler in production. A
handler receives the persisted task, including its JSON text payload.

Each task can specify `timeoutSeconds`; otherwise the configured default is
120 seconds. On timeout, the poller requests cancellation with
`Future.cancel(true)` and records a task failure. Java cannot forcibly stop
code that ignores interruption. Use client-side I/O timeouts and
interruption-aware handlers. Non-interruptible code may continue occupying its
thread even after the database task is retried or failed.

Workers update `last_heartbeat` for all tracked tasks every 5 seconds by
default, using a single batched `UPDATE`. Timeouts are enforced by a separate
watchdog thread (every second by default, `heartbeat.timeout-check-interval-ms`),
so timeout precision does not depend on the heartbeat interval or on database
latency.
A separate recovery process runs every 10 seconds and considers a processing
task stale after 30 seconds without a heartbeat. Stale tasks with attempts
remaining return to `PENDING`; tasks at their attempt limit become `FAILED`.
Recovery does not increment attempts. On graceful shutdown, tasks that are still
running after `poller.shutdown-timeout-seconds` are interrupted and released
straight back to `PENDING`; this aborted attempt is not counted against
`maxAttempts`. A paused worker can miss the stale
threshold and have its task reclaimed elsewhere, so duplicate execution must
be expected.

Retries use exponential backoff from `retry.initial-delay-ms` up to
`retry.max-delay-ms`, with randomized jitter. Defaults are one second, 60
seconds, and 20% jitter. `maxAttempts` includes the first attempt and defaults
to one, so retries are opt-in. Failed rows remain available for inspection and
replay. Replay creates a new task linked through `replayOfTaskId`; the original
failure is preserved. Only the latest error is stored; per-attempt history and
stack traces are not persisted.

### Metrics and Summary Estimates

`GET /api/v1/tasks/summary` reports task totals by status, pending counts by
type, lifecycle counts and average successful runtime by type, processing
rate, active threads, pool size, and `estimatedBacklogCompletionSeconds`.
The estimate is approximately:

```text
(pending tasks + processing tasks) * process-local average runtime
-------------------------------------------------------------------
                    configured core pool size
```

It is `null` until the process has successful runtime samples. Scheduled tasks
are excluded. The estimate does not account for future submissions, task-type
runtime differences, priority, or remaining time on current tasks. Queue
counts come from PostgreSQL; processing rate and runtime samples are local to
each process. Multi-worker estimates are not a cluster-wide capacity forecast.

Spring Actuator exposes health, metrics, and Prometheus endpoints. Micrometer
records enqueue, completion, failure, and poll counters; per-type counters;
processing timers (including aggregate p50/p95/p99 percentiles); per-type
timers; and active-processing, executor-pool, active-thread, and local-queue
gauges. `tasks.failed` counts terminal handler/timeout failures, not each
intermediate retry. Tasks marked failed by bulk stale recovery appear in
database summaries but do not currently increment Micrometer failure counters.

### Operational Limitations

- PostgreSQL is the only queue storage implementation; there is no separate
  broker adapter.
- Finished tasks (`COMPLETED`, `FAILED`, `CANCELLED`) are purged after
  `retention.retention-hours` (default `0`: on the next purge run, every 5
  minutes), so finished tasks, including failed ones, disappear quickly and can
  no longer be fetched or replayed; raise it (e.g. `168` for 7 days) if you
  rely on status lookups or replay.
  Purging a task also removes its idempotency key, so the same key can create a
  new task afterwards.
- Idempotency keys apply to single-task creation, not bulk submissions.
- The REST API has no built-in authentication or authorization.
- Recurring schedules support fixed-delay intervals, not cron or calendar
  schedules.
- Running tasks cannot be cancelled through the task API; timeout handling
  requests best-effort interruption.
- Strict priority can starve lower-priority work; fairness only covers types
  present in the bounded candidate pool.
- Metrics and runtime averages are process-local; there is no shared worker
  registry or cross-node CPU/memory view.
- Queueing and retries provide at-least-once execution, not exactly-once
  external side effects.

## Creating Custom Task Handlers

Implement `TaskHandler` as a Spring bean. The processor routes each task by
`getTaskType()` and passes the persisted task, including its payload, to the
matching handler:

```java
import com.taskprocessor.entity.Task;
import com.taskprocessor.service.TaskHandler;
import org.springframework.stereotype.Component;

@Component
public class PrintTaskHandler implements TaskHandler {

    @Override
    public String getTaskType() {
        return "print";
    }

    @Override
    public void handle(Task task) {
        System.out.printf("Task %s payload: %s%n", task.getId(), task.getPayload());
    }
}
```

Then submit a task with that `taskType`:

```json
{
  "taskType": "print",
  "payload": "{\"message\": \"Hello\"}"
}
```
