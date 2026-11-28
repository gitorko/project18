--liquibase formatted sql

--changeset taskprocessor:007-task-correlation-id
ALTER TABLE tasks ADD COLUMN correlation_id VARCHAR(100);

--rollback ALTER TABLE tasks DROP COLUMN correlation_id;
