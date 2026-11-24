--liquibase formatted sql

--changeset taskprocessor:003-map-numeric-priorities
-- Normalize the old -100..100 numeric priority range to LOW/MEDIUM/HIGH ranks.
UPDATE tasks
SET priority = CASE
    WHEN priority < 0 THEN -1
    WHEN priority IS NULL OR priority = 0 THEN 0
    ELSE 1
END
WHERE priority IS NULL OR priority NOT IN (-1, 0, 1);

ALTER TABLE tasks ALTER COLUMN priority SET DEFAULT 0;
ALTER TABLE tasks ADD CONSTRAINT chk_tasks_priority CHECK (priority IN (-1, 0, 1));

--rollback ALTER TABLE tasks DROP CONSTRAINT chk_tasks_priority;
