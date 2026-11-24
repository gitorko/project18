--liquibase formatted sql

--changeset taskprocessor:004-recurring-schedules
CREATE TABLE task_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type VARCHAR(255) NOT NULL,
    payload TEXT,
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority IN (-1, 0, 1)),
    max_attempts INTEGER NOT NULL DEFAULT 1,
    timeout_seconds INTEGER,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
    next_run_at TIMESTAMP WITH TIME ZONE,
    active_task_id UUID,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE
);

ALTER TABLE tasks ADD COLUMN schedule_id UUID;
ALTER TABLE tasks ADD CONSTRAINT fk_tasks_schedule
    FOREIGN KEY (schedule_id) REFERENCES task_schedules(id);
ALTER TABLE task_schedules ADD CONSTRAINT fk_schedule_active_task
    FOREIGN KEY (active_task_id) REFERENCES tasks(id);

CREATE INDEX idx_task_schedules_due
    ON task_schedules (next_run_at ASC)
    WHERE enabled = TRUE AND active_task_id IS NULL;
CREATE INDEX idx_tasks_schedule_id ON tasks (schedule_id);

--rollback DROP INDEX idx_tasks_schedule_id;
--rollback DROP INDEX idx_task_schedules_due;
--rollback ALTER TABLE task_schedules DROP CONSTRAINT fk_schedule_active_task;
--rollback ALTER TABLE tasks DROP CONSTRAINT fk_tasks_schedule;
--rollback ALTER TABLE tasks DROP COLUMN schedule_id;
--rollback DROP TABLE task_schedules;
