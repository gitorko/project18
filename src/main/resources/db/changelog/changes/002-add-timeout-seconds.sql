--liquibase formatted sql

--changeset taskprocessor:3-add-timeout-seconds
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'tasks' AND column_name = 'timeout_seconds'
ALTER TABLE tasks ADD COLUMN timeout_seconds INTEGER;

COMMENT ON COLUMN tasks.timeout_seconds IS 'Max execution time in seconds before the task is cancelled and failed; falls back to the configured default when null';

--rollback ALTER TABLE tasks DROP COLUMN timeout_seconds;
