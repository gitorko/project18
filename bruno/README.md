# Project18 Bruno collection

Import this directory in Bruno and start the API at `http://localhost:8080`.
Requests use that URL directly.

## Task acceptance and backpressure

`Create Task` sends an `Idempotency-Key`. Repeating the request with the same
key and body returns the existing task rather than enqueueing a duplicate. Use a
new key for different work. Bulk task creation does not support idempotency
keys.

Use `Get Tasks Summary` to inspect pending counts and the estimated backlog
completion time before submitting more work. If an admission limit is reached,
task submission returns `503 Service Unavailable` with `Retry-After: 5`; clients
should wait and retry rather than continuously resubmitting. The 503 response
depends on the configured queue limits being reached, so the normal collection
does not force that condition.

## Retry and replay

`Create Task` opts into retries with `maxAttempts`. After all attempts fail, the
task stays failed and is not picked up by normal polling. To explicitly run it
again, set the failed task ID in `Replay Failed Task`. Replay creates a new task
record; replace the example UUID with a real failed task ID.

## Schedules

`Create Recurring Schedule` creates a fixed interval schedule. Use the returned
schedule ID with `Get Schedule By Id` and `Cancel Schedule`. Replace their
example UUID with the ID returned by schedule creation.
