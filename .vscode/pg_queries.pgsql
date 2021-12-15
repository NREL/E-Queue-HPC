SELECT * FROM pg_stat_activity WHERE datname = 'dmpapps'  ORDER BY query;
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'dmpapps' and usename = 'dmpappsops' AND pid <> pg_backend_pid();


CREATE TABLE IF NOT EXISTS test_job_queue (
                queue         smallint NOT NULL,
                status        smallint NOT NULL DEFAULT 0,
                priority      int NOT NULL,
                id            uuid NOT NULL PRIMARY KEY,
                start_time    timestamp,
                update_time   timestamp,
                worker        uuid,
                error_count   smallint,
                error         text
            );


CREATE INDEX IF NOT EXISTS test_job_queue_priority_idx ON test_job_queue (queue, priority) WHERE status = 0;
CREATE INDEX IF NOT EXISTS test_job_queue_update_idx ON test_job_queue (queue, status, update_time) WHERE status <> 0;

SELECT
    indexname,
    indexdef
FROM
    pg_indexes
WHERE
    tablename = 'test_job_status';


CREATE TABLE IF NOT EXISTS test_job_data (
                id            uuid NOT NULL PRIMARY KEY,
                parent        uuid,
                depth         smallint,
                command       jsonb
            );

CREATE INDEX IF NOT EXISTS test_job_data_parent_idx ON test_job_data (parent, id) WHERE PARENT IS NOT NULL;

