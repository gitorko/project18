--liquibase formatted sql

--changeset taskprocessor:006-queue-index-tuning
-- Dequeue index: the partial predicate already pins status = 'PENDING', so status as a key
-- column only wastes space. Covers the dequeue ordering and includes scheduled_at so the
-- due-time filter can be evaluated from the index.
CREATE INDEX idx_tasks_pending_queue
ON tasks (priority DESC, created_at ASC) INCLUDE (scheduled_at)
WHERE status = 'PENDING';
DROP INDEX IF EXISTS idx_tasks_status_priority_created;

-- Replaces the low-cardinality idx_tasks_status. Admission control and summary counts only
-- look at outstanding work, so index just those rows: terminal rows never touch it.
CREATE INDEX idx_tasks_outstanding
ON tasks (task_type, status)
WHERE status IN ('PENDING', 'PROCESSING', 'SCHEDULED');
DROP INDEX IF EXISTS idx_tasks_status;

-- Supports the retention purge of finished tasks.
CREATE INDEX idx_tasks_terminal_updated
ON tasks (updated_at)
WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED');

--rollback DROP INDEX idx_tasks_terminal_updated;
--rollback CREATE INDEX idx_tasks_status ON tasks (status);
--rollback DROP INDEX idx_tasks_outstanding;
--rollback CREATE INDEX idx_tasks_status_priority_created ON tasks (status, priority DESC, created_at ASC) WHERE status = 'PENDING';
--rollback DROP INDEX idx_tasks_pending_queue;
