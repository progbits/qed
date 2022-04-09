CREATE TABLE IF NOT EXISTS qed_job
(
    job_id       TEXT PRIMARY KEY,
    queue        TEXT NOT NULL,
    data         JSONB,
    delivered    BOOLEAN default false,
    delivered_at TIMESTAMPTZ,
    not_before   TIMESTAMPTZ default now(),
    created_at   TIMESTAMPTZ default now()
);

-- Enqueue a new job with data onto the named queue.
CREATE OR REPLACE FUNCTION qed_enqueue(
    id TEXT,
    queue_name TEXT,
    job_data JSONB,
    run_after TIMESTAMPTZ default now()) RETURNS VOID AS
    $$
    BEGIN
        INSERT INTO qed_job(job_id, queue, data, not_before)
        VALUES (id, queue_name, job_data, run_after);
    END;
    $$
    LANGUAGE 'plpgsql';

-- Atomically mark a job as 'out for delivery'.
CREATE OR REPLACE FUNCTION qed_dequeue()
    RETURNS TABLE(job_id TEXT, queue TEXT, data JSONB) AS
        $$
        BEGIN
            UPDATE qed_job
            SET delivered = true,
                delivered_at = now()
            WHERE job_id = (
                SELECT job_id
                FROM qed_job
                WHERE delivered = false
                    AND not_before < now()
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE
                SKIP LOCKED
            )
            RETURNING job_id, queue, data;
        END
        $$
    LANGUAGE 'plpgsql';

-- Acknowledge (delete) a job from the queue.
CREATE OR REPLACE FUNCTION qed_ack(id TEXT)
    RETURNS VOID AS
        $$
        BEGIN
            DELETE from qed_job
            WHERE job_id = id;
        END
        $$
    LANGUAGE 'plpgsql';

-- Mark any jobs that have been out for delivery for more than 60 seconds as
-- delivered = false. This allows the reclamation of crashed or stuck jobs.
CREATE OR REPLACE FUNCTION qed_unlock()
    RETURNS VOID AS
    $$
    BEGIN
        UPDATE qed_job
        SET delivered = false,
            delivered_at = NULL
        WHERE delivered = true
            AND (delivered_at - (interval '60 seconds')) < now();
    END
    $$
    LANGUAGE 'plpgsql';
