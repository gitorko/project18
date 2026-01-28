-- Task Processor Schema
-- This file is for reference and can be used for manual DB setup
-- Spring JPA will auto-create tables if ddl-auto is set to 'update' or 'create'

-- Tasks table
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type VARCHAR(255) NOT NULL,
    payload TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    priority INTEGER DEFAULT 0,
    attempt_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    worker_id VARCHAR(255),
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    scheduled_at TIMESTAMP WITH TIME ZONE
);

-- Index for efficient task polling (status + priority + created_at)
-- This is the most important index for queue performance
CREATE INDEX IF NOT EXISTS idx_tasks_status_priority_created 
ON tasks (status, priority DESC, created_at ASC) 
WHERE status = 'PENDING';

-- Index for status-based queries
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);

-- Index for worker-specific queries
CREATE INDEX IF NOT EXISTS idx_tasks_worker_id ON tasks (worker_id) 
WHERE worker_id IS NOT NULL;

-- Index for stale task detection
CREATE INDEX IF NOT EXISTS idx_tasks_heartbeat ON tasks (last_heartbeat) 
WHERE status = 'PROCESSING';

-- Index for scheduled tasks
CREATE INDEX IF NOT EXISTS idx_tasks_scheduled ON tasks (scheduled_at) 
WHERE status = 'SCHEDULED';

-- Comments for documentation
COMMENT ON TABLE tasks IS 'Task queue table for distributed task processing';
COMMENT ON COLUMN tasks.id IS 'Unique task identifier';
COMMENT ON COLUMN tasks.task_type IS 'Type of task - used for routing to handlers';
COMMENT ON COLUMN tasks.payload IS 'JSON payload with task-specific data';
COMMENT ON COLUMN tasks.status IS 'Current status: PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED, SCHEDULED';
COMMENT ON COLUMN tasks.priority IS 'Processing priority (higher = more urgent)';
COMMENT ON COLUMN tasks.attempt_count IS 'Number of processing attempts';
COMMENT ON COLUMN tasks.max_attempts IS 'Maximum retry attempts before marking as failed';
COMMENT ON COLUMN tasks.worker_id IS 'ID of worker currently processing this task';
COMMENT ON COLUMN tasks.last_heartbeat IS 'Last heartbeat from processing worker';
COMMENT ON COLUMN tasks.scheduled_at IS 'Scheduled time for delayed execution';
