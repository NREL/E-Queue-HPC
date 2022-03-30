SELECT * FROM pg_stat_activity WHERE datname = 'dmpapps'  ORDER BY query;
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'dmpapps' and usename = 'dmpappsops' AND pid <> pg_backend_pid();


CREATE TABLE IF NOT EXISTS test2_job_queue (
                id            bigserial NOT NULL PRIMARY KEY,
                queue         smallint NOT NULL,
                status        smallint NOT NULL DEFAULT 0,
                priority      int NOT NULL,
                start_time    timestamp,
                update_time   timestamp,
                worker        bigint,
                error_count   smallint,
                error         text
            );
CREATE INDEX IF NOT EXISTS test2_job_queue_priority_idx ON test2_job_queue (queue, priority) WHERE status = 0;
CREATE INDEX IF NOT EXISTS test2_job_queue_update_idx ON test2_job_queue (queue, status, update_time) WHERE status <> 0;

CREATE TABLE IF NOT EXISTS test2_job_data (
                id            bigint NOT NULL PRIMARY KEY,
                parent        bigint,
                depth         smallint,
                command       jsonb
            );

CREATE INDEX IF NOT EXISTS test2_job_data_parent_idx ON test2_job_data (parent, id) WHERE PARENT IS NOT NULL;



WITH t AS (
    SELECT nextval('test2_job_queue_id_seq'::regclass) as id, queue::SMALLINT, depth::SMALLINT, priority::int, parent::bigint, command::jsonb
    FROM (VALUES (0, NULL, 1234, NULL, NULL), (0, NULL, 1234, NULL, NULL), (0, NULL, 5678, NULL, NULL)) AS t (queue, depth, priority, parent, command)),
    v AS (INSERT INTO test2_job_queue (id, queue, priority) (SELECT id, queue, priority FROM t))
INSERT INTO test2_job_data (id, parent, depth, command) 
    (SELECT id, parent, depth, command FROM t) RETURNING id;