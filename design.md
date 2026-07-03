# How the Task Processor Works

This is a companion to `README.md` (API reference / config) that explains the
runtime mechanics end to end: how a task moves from submission to completion,
how many pods can safely share one queue, and how fairness, priority, and
timeouts interact.

## 1. Submission

`POST /api/v1/tasks` (or `/bulk`) inserts a row into the `tasks` table with
status `PENDING` (or `SCHEDULED` if `scheduledAt` is in the future). There is
no cap on how many tasks can be queued — submission never blocks or rejects
based on queue depth.

Two fields are defaulted at creation time if the caller doesn't set them
(`TaskQueueService.enqueueTask` / `enqueueTasks`):

- `maxAttempts` → defaults to **1** (no retry) if omitted. Retries are opt-in:
  you only get retried on failure if you explicitly ask for `maxAttempts > 1`
  when creating the task.
- `timeoutSeconds` → defaults to `task-processor.poller.default-task-timeout-seconds`
  (120s) if omitted.

## 2. Dequeue: SKIP LOCKED for distributed safety

Every pod runs a `TaskPollerService` that polls on a schedule
(`poller.poll-interval-ms`, adaptive — see below). Each poll runs one SQL
query (`TaskRepository.findTasksForProcessing`):

```sql
SELECT * FROM tasks
WHERE status = 'PENDING' AND scheduled_at <= now
ORDER BY priority DESC, created_at ASC
LIMIT :candidatePoolSize
FOR UPDATE SKIP LOCKED
```

`FOR UPDATE SKIP LOCKED` locks the rows it returns and skips any row another
transaction (i.e. another pod, or another poll on the same pod) already has
locked. This is what guarantees **two pods can never pick the same task** —
there's no separate coordination service, leader election, or distributed
lock needed; Postgres row locks do it.

## 3. Fair selection within the candidate pool

The query above doesn't fetch just `batchSize` rows — it over-fetches a
**candidate pool** (`batchSize * fairnessCandidateMultiplier`, capped at
`maxFairnessCandidates`). `TaskQueueService.selectFairBatch` then picks the
actual batch from that pool with two rules, in this order:

1. **Priority strictly wins.** Tasks are grouped by priority tier in the order
   the SQL already returned them (highest priority first). A lower-priority
   tier is only considered once the current tier's candidates are fully
   drained *and* the batch still has room. So if there are 10 low-priority
   `taskA` and 10 high-priority `taskB` pending, `taskB` is always picked
   first, every time — priority is never overridden by fairness.
2. **Round-robin by task type within a tier.** Inside a single priority
   tier, instead of pure FIFO (oldest `created_at` first), one task is taken
   from each distinct `taskType` in turn, cycling through types, each type's
   own tasks staying in FIFO order. This is what stops a heavily-enqueued
   type (e.g. `taskA` submitted thousands of times) from starving a
   less-frequent type (`taskB`) queued at the same priority — without this,
   pure `created_at ASC` ordering would let `taskA` dominate every batch. If
   there are several high-priority types, they round-robin among
   *themselves* once the tier is reached.

Rows in the candidate pool that aren't selected simply aren't updated; their
row lock releases when the transaction commits, same as if they'd never been
fetched — they remain `PENDING` and eligible for the next poll (by this pod
or another).

Selected rows are marked `PROCESSING`, stamped with this pod's `workerId`,
`startedAt`, and `lastHeartbeat`, and `attemptCount` is incremented.

## 4. Execution and concurrency

Each selected task is submitted to a per-pod `ThreadPoolExecutor`
(`taskProcessorExecutor`). The pool size is what controls how many tasks this
pod runs **in parallel** — on a small VM you'd run a small pool; scale
horizontally by adding pods (each with its own pool), and/or vertically by
increasing `poller.pool-size` / `poller.max-pool-size` at runtime via
`PUT /api/v1/poller/config`. A task is routed to a `TaskHandler` by
`taskType` (`TaskProcessorService`).

## 5. Heartbeat, timeout, and crash recovery

Two independent failure modes are handled differently:

- **Crashed worker** (the pod dies mid-task): a background heartbeat updates
  `last_heartbeat` every `heartbeat.interval-ms` for every task the pod is
  currently running. `StaleTaskRecoveryService` periodically looks for
  `PROCESSING` tasks whose heartbeat has gone stale (`heartbeat.stale-threshold-ms`)
  — that pod is presumed dead, and the task is reset to `PENDING` for another
  pod to pick up (or `FAILED` if `attemptCount >= maxAttempts`).

- **Hung-but-alive task** (the handler itself is stuck, worker is fine): the
  same per-tick pass that sends heartbeats also checks each in-flight task's
  elapsed time against its effective timeout (`task.timeoutSeconds`, or the
  pod-wide default). If a task has run longer than its timeout, the poller:
  1. atomically removes it from its own in-memory tracking map (so a
     concurrent normal completion can't also try to finalize it),
  2. calls `Future.cancel(true)` on the task's execution — this sends a
     thread interrupt, and
  3. marks the task failed via the normal `failTask` path with a timeout
     error message.

  **Caveat:** `cancel(true)` only *requests* interruption. If a handler
  blocks on I/O or a loop that never checks `Thread.interrupted()`, the
  thread can keep running past the timeout and the pool slot stays occupied
  until it eventually returns. Handlers should use interruptible operations
  (e.g. HTTP clients with timeouts, periodic interrupt checks in loops) for
  the timeout to actually free up capacity.

Because both paths (a normal completion/failure from inside `processTask`,
and a timeout from the watchdog) can race to write the final status,
`TaskQueueService.completeTask` / `failTask` both start with a guard: if the
row is no longer `PROCESSING` (another path already resolved it, or another
pod has since re-dequeued it), the write is a no-op. Only one outcome ever
sticks.

## 6. Retry vs. fail

`failTask` retries a task (resets to `PENDING`) if `attemptCount < maxAttempts`,
otherwise marks it `FAILED`. Since `maxAttempts` defaults to 1 at creation
time, **the default behavior for any failure — including a timeout — is to
fail immediately**, not retry. Retrying is something a caller opts into per
task by setting `maxAttempts` above 1 when creating it.

## 7. Adaptive polling

When a poll finds no tasks, the poll interval doubles (up to
`poller.max-poll-interval-ms`) to reduce idle DB load; as soon as tasks are
found again, it drops back to `poller.min-poll-interval-ms`. This is
per-pod and independent of the fairness/priority logic above.

## 8. Monitoring

`GET /api/v1/tasks/summary` and `/actuator/prometheus` expose queue depth
(overall and by type), processing rate, active workers, and pool size — use
these to notice a growing backlog before it becomes an incident, and to
decide whether to bump `poolSize` on existing pods or add more pods.
