--liquibase formatted sql

--changeset taskprocessor:008-hot-table-tuning
-- Stale recovery looks for PROCESSING rows by heartbeat; idx_tasks_outstanding leads with task_type.
CREATE INDEX idx_tasks_processing_heartbeat
ON tasks (last_heartbeat)
WHERE status = 'PROCESSING';

-- Tasks are updated several times per life (claim, heartbeats, finalize). Leave page room so
-- those updates can be HOT, and vacuum the churning table more eagerly than the 20% default.
ALTER TABLE tasks SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0.05
);

--rollback ALTER TABLE tasks RESET (fillfactor, autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold, autovacuum_analyze_scale_factor);
--rollback DROP INDEX idx_tasks_processing_heartbeat;
