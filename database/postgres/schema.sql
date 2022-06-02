CREATE TABLE IF NOT EXISTS qed_task (
    task_id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    data bytea,
    delivered BOOLEAN DEFAULT FALSE,
    delivered_at timestamptz,
    not_before timestamptz DEFAULT now(),
    created_at timestamptz DEFAULT now()
);

-- Return the number of pending tasks.
CREATE OR REPLACE VIEW qed_size AS
SELECT
    count(*)
FROM
    qed_task;

-- Enqueue a new task onto the named queue.
CREATE OR REPLACE FUNCTION qed_enqueue (
    id TEXT,
    queue_name TEXT,
    task_data BYTEA,
    delay INT DEFAULT 0)
    RETURNS VOID
AS $$
BEGIN
    INSERT INTO qed_task (task_id, queue, data, not_before)
    VALUES (id, queue_name, task_data, now() + interval '$4 seconds');
END;
$$
    LANGUAGE 'plpgsql';

-- Atomically select and mark a task as delivered.
CREATE OR REPLACE FUNCTION qed_dequeue ()
    RETURNS TABLE (task_id text, queue text, data bytea)
AS $$
UPDATE
    qed_task
SET
    delivered = TRUE,
    delivered_at = now()
WHERE
    qed_task.task_id = (
        SELECT
            qed_task.task_id
        FROM
            qed_task
        WHERE
            delivered = FALSE
            AND
            not_before < now()
        ORDER BY
            created_at DESC
        LIMIT 1
        FOR UPDATE
        SKIP LOCKED
    )
RETURNING
    qed_task.task_id,
    qed_task.queue,
    qed_task.data;
$$
    LANGUAGE 'sql';

-- Delete a task from the queue.
CREATE OR REPLACE FUNCTION qed_ack (id text)
    RETURNS VOID
AS $$
BEGIN
    DELETE FROM qed_task
    WHERE task_id = id;
END
$$
    LANGUAGE 'plpgsql';

-- Mark any tasks that have been out for delivery for longer than the
-- specified interval as `delivered = false`.
CREATE OR REPLACE FUNCTION qed_unlock (t INTEGER)
    RETURNS VOID
AS $$
BEGIN
    UPDATE
        qed_task
    SET
        delivered = FALSE,
        delivered_at = NULL
    WHERE
        delivered = TRUE
        AND
        (delivered_at + (interval '$1 seconds')) < now();
END
$$
    LANGUAGE 'plpgsql';
