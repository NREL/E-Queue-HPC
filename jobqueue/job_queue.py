import abc
import json
import os
import platform
import random
import time
import traceback
from typing import Callable, Dict, Iterator, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2 import sql
import psycopg2.extras

from jobqueue.job_status import JobStatus

from .job import Job
from .cursor_manager import CursorManager


class JobQueue:

    _database: str
    _credentials: Dict[str, any]
    _queue_id: int

    _status_table: str
    _data_table: str

    def __init__(
        self,
        credentials: Dict[str, any],
        queue: int = 0,
        check_table=False,
        drop_table=False,
    ) -> None:
        """ Interface to the jobsque database table
        database: str, name of the key in your .jobqueue.json file.
        queue: str, name of the queue you'd like to create or use.
        _table_name is use for testing only.  To change your table name use the
        .jobqueue.json file.
        """
        credentials = credentials.copy()
        table_base_name = credentials['table_name']
        del credentials['table_name']

        self._credentials = credentials
        self._queue_id = queue
        self._status_table = table_base_name + '_status'
        self._data_table = table_base_name + '_data'

        # ensure table exists
        if check_table:
            self._create_tables(drop_table=drop_table)

    def clear(self) -> None:
        """ Clears all records for this queue.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("DELETE FROM {status_table} WHERE queue = %s;").format(
                status_table=sql.Identifier(self._status_table)),
                [self._queue_id])

    def pop(self, worker_id: Optional[int] = None) -> Optional[Job]:
        """ 
        Claims and returns one job.
        Returns None if no job could be claimed.
        An optional worker id can be assigned. 
        """
        result = self.pop_multiple(worker_id, num_jobs=1)
        return None if len(result) == 0 else result[0]

    def pop_multiple(self, worker_id: Optional[int] = None, num_jobs: int = 1) -> Optional[Job]:
        """ 
        Claims and returns up to the requested number of jobs.
        An optional worker id can be assigned. 
        """

        host = platform.node()

        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
WITH p AS (
SELECT id, priority FROM {status_table}
    WHERE 
        queue = %(queue)s AND
        status = {queued_status}
    ORDER BY priority ASC
    LIMIT %(num_jobs)s FOR UPDATE SKIP LOCKED),
u AS (
UPDATE {status_table} as q
    SET 
        status = {claimed_status},
        worker = %(worker_id)s,
        start_time = NOW(),
        update_time = NOW()
    FROM p
    WHERE p.id = q.id)
SELECT 
    p.id AS id, 
    p.priority AS priority,
    t.command AS command
FROM
p,
{data_table} as t
WHERE
p.id = t.id;""").format(
                status_table=sql.Identifier(self._status_table),
                data_table=sql.Identifier(self._data_table),
                queued_status=sql.Literal(JobStatus.Queued.value),
                claimed_status=sql.Literal(JobStatus.Claimed.value),
            ), {
                'queue': self._queue_id,
                'worker_id': worker_id,
                'num_jobs': num_jobs,
            })
            return [Job(
                id=result[0],
                priority=result[1],
                command=result[4],
            )
                for result in cursor.fetchall()]

    def push(self, job: Job) -> None:
        """ Adds a job to the queue.
        """
        self.push_multiple((job,))

    def push_multiple(self, jobs: Iterator[Job]) -> None:
        """ Adds jobs to the queue.
        """
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        with CursorManager(self._credentials) as cursor:
            # cursor, insert_query, data, template=None, page_size=100
            command = sql.SQL("""
WITH t AS (
SELECT 
    nextval({status_table_id_sequence}::regclass) as id, 
    priority::int,
    command::jsonb
    FROM (VALUES %s) AS t (priority, command)),
v AS (INSERT INTO {status_table} (id, queue, priority) (SELECT id, {queue}, priority FROM t))
INSERT INTO {data_table} (id, command)
    (SELECT id, command FROM t) RETURNING id;""").format(
                status_table_id_sequence=sql.Literal(
                    self._status_table + '_id_seq'),
                queue=sql.Literal(self._queue_id),
                status_table=sql.Identifier(self._status_table),
                data_table=sql.Identifier(self._data_table),
            )

            psycopg2.extras.execute_values(
                cursor,
                command,
                (
                    (
                        j.priority,
                        json.dumps(j.command, separators=(',', ':'))
                    )
                    for j in jobs),
                template=None,
                page_size=128,
            )

    def update_job(self, job_id: int) -> None:
        """ While a job is being worked on, the worker can periodically let the queue know it is still working on the
            job (instead of crashed or frozen).
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
UPDATE {status_table}
SET update_time = NOW()
WHERE id = %s;""").format(
                status_table=sql.Identifier(self._status_table)),
                [job_id])

    def mark_job_complete(self, job_id: int) -> None:
        """ When a job is finished, this function will mark the status as done.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
UPDATE {status_table}
SET status = {complete_status},
    update_time = NOW()
WHERE id = %s;""").format(
                status_table=sql.Identifier(self._status_table),
                complete_status=sql.Literal(JobStatus.Complete.value),
            ),
                [job_id])

    def mark_job_failed(self, job_id: int, error: str) -> None:
        """ When a job failed, this function will mark the status as failed.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
UPDATE {status_table}
SET status = {failed_status},
    update_time = NOW(),
    error = %s,
    error_count = COALESCE(error_count, 0) + 1
WHERE id = %s;""").format(
                status_table=sql.Identifier(self._status_table),
                failed_status=sql.Literal(JobStatus.Failed.value),
            ),
                [error, job_id])

    def run_worker(self, handler: Callable[[int, Job], None], wait_until_exit=15 * 60,
                   maximum_waiting_time=5 * 60):
        print(f"Job Queue: Starting...")

        worker_id = int(datetime.utcnow().timestamp() * 8192)
        wait_start = None
        wait_bound = 1.0
        while True:

            # Pull job off the queue
            jobs = self.pop(worker=worker_id)

            if len(jobs) == 0:

                if wait_start is None:
                    wait_start = time.time()
                    wait_bound = 1
                else:
                    waiting_time = time.time() - wait_start
                    if waiting_time > wait_until_exit:
                        print(
                            "Job Queue: No Jobs, max waiting time exceeded. Exiting...")
                        break

                # No jobs, wait and try again.
                print("Job Queue: No jobs found. Waiting...")

                # bounded randomized exponential backoff
                wait_bound = min(maximum_waiting_time, wait_bound * 2)
                time.sleep(random.uniform(1.0, wait_bound))
                continue

            job = jobs[0]
            try:
                wait_start = None

                print(f"Job Queue: {job.id} running...")

                handler(worker_id, job)  # handle the message
                # Mark the job as complete in the self.
                self.mark_job_complete(job)

                print(f"Job Queue: {job.id} done.")
            except Exception as e:
                print(
                    f"Job Queue: {job.id} unhandled exception {e} in jq_runner.")
                print(traceback.format_exc())
                try:
                    self.mark_job_failed(job, str(e))
                except Exception as e2:
                    print(
                        f"Job Queue: {job.id} exception thrown while marking as failed in jq_runner: {e}, {e2}!")
                    print(traceback.format_exc())

    @property
    def message_counts(self) -> Tuple[int, int, int, int]:
        """ Returns the count of open, pending, and completed jobs for a given group.
        Output: (num open, num pending, num completed)
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
SELECT 
    (SELECT COUNT(*) FROM {status_table} 
        WHERE queue = %s AND
            status = {queued_status}) AS open,
    (SELECT COUNT(*) FROM {status_table} 
        WHERE queue = %s AND
            status = {claimed_status}) AS running,
    (SELECT COUNT(*) FROM {status_table} 
        WHERE queue = %s AND
            status = {complete_status}) AS done,
    (SELECT COUNT(*) FROM {status_table} 
        WHERE queue = %s AND
            status = {failed_status}) AS failed;""").format(
                status_table=sql.Identifier(self._status_table),
                queued_status=sql.Literal(JobStatus.Queued.value),
                claimed_status=sql.Literal(JobStatus.Claimed.value),
                complete_status=sql.Literal(JobStatus.Complete.value),
                failed_status=sql.Literal(JobStatus.Failed.value),
            ),
                [self._queue_id])
            records = cursor.fetchone()
            return tuple(records)

    def fail_incomplete_jobs(self, interval='12 hours') -> None:
        """ Mark all incomplete jobs in a given group started more than {interval} ago as failed.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
UPDATE {status_table}
    SET update_time = NOW()
    WHERE queue = %s
        AND status = {claimed_status}
        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;""").format(
                status_table=sql.Identifier(self._status_table),
                claimed_status=sql.Literal(JobStatus.Claimed.value),
            ),
                [self._queue_id, interval])

    def reset_incomplete_jobs(self, interval='12 hours') -> None:
        """ Mark all incomplete jobs in a given group started more than {interval} ago as open.
        """

        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
UPDATE {status_table}
    SET 
        status = {queued_status},
        start_time = NULL, 
        update_time = NULL, 
        error = NULL,
        error_count = COALESCE(error_count, 0) + 1
    WHERE queue = %s
        AND status = {claimed_status}
        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;""").format(
                status_table=sql.Identifier(self._status_table),
                queued_status=sql.Literal(JobStatus.Queued.value),
                claimed_status=sql.Literal(JobStatus.Claimed.value),
            ),
                [self._queue_id, interval])

    def reset_failed_jobs(self, interval='0 hours') -> None:
        """ Mark all failed jobs in a given group started more than {interval} ago as open.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
UPDATE {status_table}
    SET status = {queued_status},
        start_time = NULL, 
        update_time = NULL, 
        end_time = NULL, 
        error = NULL
    WHERE queue = %s
        AND status = {failed_status}
        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;""").format(
                status_table=sql.Identifier(self._status_table),
                queued_status=sql.Literal(JobStatus.Queued.value),
                failed_status=sql.Literal(JobStatus.Failed.value),
            ),
                [self._queue_id, interval])

    def _create_tables(self, drop_table: bool = False) -> None:
        """ Deletes and replaces or creates the current jobqueue table.
        """
        with CursorManager(self._credentials, autocommit=False) as cursor:
            if drop_table:
                cursor.execute(sql.SQL("""
DROP TABLE IF EXISTS {status_table};
DROP TABLE IF EXISTS {data_table};""").format(
                    status_table=sql.Identifier(self._status_table),
                    data_table=sql.Identifier(self._data_table),
                ))

            cursor.execute(sql.SQL("""
CREATE TABLE IF NOT EXISTS {status_table} (
    queue         smallint NOT NULL,
    status        smallint NOT NULL DEFAULT {queued_status},
    priority      int NOT NULL,
    id            bigserial NOT NULL PRIMARY KEY,
    start_time    timestamp,
    update_time   timestamp,
    worker        bigint,
    error_count   smallint,
    error         text
);
CREATE INDEX IF NOT EXISTS {priority_index} ON {status_table} (queue, priority) WHERE status = {queued_status};
CREATE INDEX IF NOT EXISTS {update_index} ON {status_table} (queue, status, update_time) WHERE status > {queued_status};

CREATE TABLE IF NOT EXISTS {data_table} (
    id            bigint NOT NULL PRIMARY KEY,
    command       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS {parent_index} ON {data_table} (parent, id) WHERE parent IS NOT NULL;""").format(
                status_table=sql.Identifier(self._status_table),
                data_table=sql.Identifier(self._data_table),
                priority_index=sql.Identifier(
                    self._status_table + '_priority_idx'),
                update_index=sql.Identifier(
                    self._status_table + '_update_idx'),
                parent_index=sql.Identifier(self._data_table + '_parent_idx'),
                queued_status=sql.Literal(JobStatus.Queued.value),
            ))

            cursor.connection.commit()

    def get_jobs_as_dataframe(self) -> pd.DataFrame:
        """ Returns all queues as a dataframe.
        """
        with CursorManager(self._credentials) as cursor:
            command = sql.SQL("""
SELECT 
    s.queue as queue,
    s.status as status,
    s.priority as priority,
    s.id as id,
    s.start_time as start_time,
    s.update_time as update_time,
    s.worker as worker,
    s.error_count as error_count,
    s.error as error,
    d.parent as parent,
    d.depth as depth,
    d.command as command
FROM 
    {status_table} AS s,
    {data_table} AS d
WHERE
    s.id = d.id AND
    queue = %(queue)s;""").format(
                status_table=sql.Identifier(self._status_table),
                data_table=sql.Identifier(self._data_table))

            return pd.read_sql(
                command,
                params={'queue': self._queue_id},
                con=cursor.connection,
            )

    def get_queue_length(self) -> int:
        """ Returns the count of open jobs for a given group.
        Output: int, number of open jobs
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
SELECT COUNT(*) FROM {status_table} 
    WHERE 
        queue = %s AND
        status = {queued_status};""").format(
                status_table=sql.Identifier(self._status_table),
                queued_status=sql.Literal(JobStatus.Queued.value),
            ),
                [self._queue_id])
            return cursor.fetchone()
