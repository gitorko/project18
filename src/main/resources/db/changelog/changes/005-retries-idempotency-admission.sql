--liquibase formatted sql

--changeset taskprocessor:005-retries-idempotency-admission
ALTER TABLE tasks ADD COLUMN replay_of_task_id UUID;
ALTER TABLE tasks ADD CONSTRAINT fk_tasks_replay_source
    FOREIGN KEY (replay_of_task_id) REFERENCES tasks(id);

CREATE TABLE task_idempotency (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL,
    task_id UUID NOT NULL UNIQUE,
    CONSTRAINT fk_idempotency_task FOREIGN KEY (task_id) REFERENCES tasks(id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE queue_admission_gate (
    id INTEGER PRIMARY KEY
);
INSERT INTO queue_admission_gate (id) VALUES (1);

--rollback DROP TABLE queue_admission_gate;
--rollback DROP TABLE task_idempotency;
--rollback ALTER TABLE tasks DROP CONSTRAINT fk_tasks_replay_source;
--rollback ALTER TABLE tasks DROP COLUMN replay_of_task_id;
