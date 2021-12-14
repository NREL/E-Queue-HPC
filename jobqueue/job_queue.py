import abc
import json
import os
import random
import time
import traceback
import uuid
from typing import Optional, Callable
from typing import Dictionary, Tuple

import json
import os
import platform
import time
import uuid
from typing import Optional, Iterator

import pandas as pd
from psycopg2 import sql
import psycopg2

from cursor_manager import CursorManager

from . import functions
from .job import Job


class JobQueue:

    _database: str
    _credentials: Dictionary[str, any]
    _queue_id: int
    # self.queue_table: str = _table_name + '_queue'
    _queue_table: str
    _data_table: str

    def __init__(
        self,
        credentials: Dictionary[str, any],
        queue: int = 0,
        check_table=False,
    ) -> None:
        """ Interface to the jobsque database table
        database: str, name of the key in your .jobsself.json file.
        queue: str, name of the queue you'd like to create or use.
        _table_name is use for testing only.  To change your table name use the
        .jobsself.json file.
        """
        credentials = credentials.copy()
        table_base_name = credentials['table_name']
        del credentials['table_name']

        self._credentials = credentials
        self._queue_id = queue
        self._queue_table = table_base_name + '_status'
        self._data_table = table_base_name + '_data'

        # ensure table exists
        if check_table:
            self._create_tables(self._credentials, self._table_name)

    @property
    def messages(self) -> int:
        res = self.get_queue_length(
            self._credentials, self._table_name, self._queue)
        return res[0]

    @property
    def message_counts(self) -> Tuple[int, int, int, int]:
        """ Returns the count of open, pending, and completed jobs for a given group.
        Output: (num open, num pending, num completed)
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                    SELECT 
                        (SELECT COUNT(*) FROM {queue_table} 
                            WHERE queue = %s AND
                                status = 0) AS open,
                        (SELECT COUNT(*) FROM {queue_table} 
                            WHERE queue = %s AND
                                status = 1) AS running,
                        (SELECT COUNT(*) FROM {queue_table} 
                            WHERE queue = %s AND
                                status = 2) AS done,
                        (SELECT COUNT(*) FROM {queue_table} 
                            WHERE queue = %s AND
                                status = -1) AS failed
                    ;
                    """
            ).format(
                queue_table=sql.Identifier(self._queue_table)),
                [self._queue_id])
            records = cursor.fetchone()
            return tuple(records)

    def clear(self) -> None:
        """ Clears all records for a given group.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("DELETE FROM {queue_table} WHERE queue = %s;").format(
                queue_table=sql.Identifier(self._queue_table)),
                [self._queue_id])

    def pop(self, worker: Optional[uuid.UUID] = None, num_jobs: int = 1) -> Optional[Job]:
        """ 
        Claims and returns jobs from the self.  
        An optional worker id can be assigned. 
        """

        host = platform.node()

        with CursorManager(self._credentials) as cursor:
            """
            + remove entry from priority queue
            + set start time
            + set worker info (optional...)

            queue
            priority
            job id
            worker id, start time

            options:
                + single table: [id, queue, priority, worker, start, update, end, retries]
                    + get highest priority
                    + update set priority = NULL, worker id, start time
                    + later, update and set end time
                + two tables: [id, queue, priority], [id, queue, worker, start, update, end, retries]
                    + get and delete highest priority
                    + insert start record into log table [start, worker, end]
                    + later, insert finish record or update start record
                + three tables: [id, queue, priority]
                    + get and delete highest priority
                    + insert start record
                        [id, start, worker]
                    + later, insert finish record
                        [id, end, worker]
            """
            cursor.execute(sql.SQL("""
            WITH p AS (
                SELECT id, priority FROM {queue_table}
                    WHERE 
                        queue = %(queue)s AND
                        status = 0
                    ORDER BY priority ASC
                    LIMIT %(num_jobs)s FOR UPDATE SKIP LOCKED),
            u AS (
                UPDATE {queue_table} as q
                    SET 
                        status = 1,
                        worker = %(worker_id)s,
                        start_time = NOW(),
                        update_time = NOW()
                    FROM p
                    WHERE q.id = q.id)
            SELECT p.id AS id, p.priority AS priority, t.config AS config
            FROM
                p,
                {data_table} as t
            WHERE
                p.id = t.id;""").format(
                queue_table=sql.Identifier(self._queue_table),
                data_table=sql.Identifier(self.data_Table),
            ), {
                'queue': self._queue_id,
                'worker_id': worker_id,
                'num_jobs': num_jobs,
            })
            return [Job(self, result[0], result[1], command=result[2]) for result in cursor.fetchall()]

    def push(self, jobs: Iterator[Job]) -> None:
        """ Adds a job (dictionary) to the database jobqueue table.
            Input:  credentials
                    group: str, name of "queue" in the database
                    job: dict, must be able to call json.dumps
            Output: None
        """
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        with CursorManager(self._credentials) as cursor:
            # cursor, insert_query, data, template=None, page_size=100
            command = sql.SQL("""
                WITH t AS (SELECT * FROM (VALUES %s) AS t (queue, priority, id, parent, depth, command),
                s AS (INSERT INTO {queue_table} (queue, priority, id)
                    (SELECT queue, priority, id FROM t)
                    ON CONFLICT DO NOTHING)
                INSERT INTO {data_table} (id, parent, depth, command)
                    (SELECT id, parent, depth, command FROM t)
                    ON CONFLICT DO NOTHING;""").format(
                queue_table=sql.Identifier(self._queue_table),
                data_table=sql.Identifier(self._data_table),
            )
            # TODO: remove queue id from here and put it into the command instead
            psycopg2.extras.execute_values(
                cursor,
                command,
                ((self._queue_id, j.queue, j.priority, j.parent, j.depth, j.command)
                 for j in jobs),
                template=None,
                page_size=128,
            )

    def fail_incomplete_jobs(self, interval='12 hours') -> None:
        """ Mark all incomplete jobs in a given group started more than {interval} ago as failed.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                UPDATE {queue_table}
                    SET update_time = NOW()
                    WHERE queue = %s
                        AND status = 1
                        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;
                """
            ).format(
                queue_table=sql.Identifier(self._queue_table)),
                [self._queue_id, interval])

    def reset_incomplete_jobs(self, interval='12 hours') -> None:
        """ Mark all incomplete jobs in a given group started more than {interval} ago as open.
        """

        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                UPDATE {queue_table}
                    SET 
                        status = 0,
                        start_time = NULL, 
                        update_time = NULL, 
                        error = NULL,
                        error_count = COALESCE(error_count, 0) + 1
                    WHERE queue = %s
                        AND status = 1
                        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;
                """
            ).format(
                queue_table=sql.Identifier(self._queue_table)),
                [self._queue_id, interval])

    def reset_failed_jobs(self, interval='0 hours') -> None:
        """ Mark all failed jobs in a given group started more than {interval} ago as open.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                        UPDATE {queue_table}
                            SET status = 0,
                                start_time = NULL, 
                                update_time = NULL, 
                                end_time = NULL, 
                                error = NULL
                            WHERE queue = %s
                                AND status = -1
                                AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;
                        """
            ).format(
                queue_table=sql.Identifier(self._queue_table)),
                [self._queue_id, interval])

    def run_worker(self, handler: Callable[[uuid.UUID, Job], None], wait_until_exit=15 * 60,
                   maximum_waiting_time=5 * 60):
        print(f"Job Queue: Starting...")

        worker_id = uuid.uuid4()
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

                print(f"Job Queue: {job.uuid} running...")

                handler(worker_id, job)  # handle the message
                # Mark the job as complete in the self.
                self.mark_job_complete(job)

                print(f"Job Queue: {job.uuid} done.")
            except Exception as e:
                print(
                    f"Job Queue: {job.uuid} unhandled exception {e} in jq_runner.")
                print(traceback.format_exc())
                try:
                    self.mark_job_failed(job, str(e))
                except Exception as e2:
                    print(
                        f"Job Queue: {job.uuid} exception thrown while marking as failed in jq_runner: {e}, {e2}!")
                    print(traceback.format_exc())

    def _create_tables(self, drop_table: bool = True) -> None:
        """ Deletes and replaces or creates the current jobqueue table.
        """
        with CursorManager(self._credentials, autocommit=False) as cursor:
            if drop_table:
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(
                    sql.Identifier(table_name)))

            cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {queue_table} (
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
            CREATE INDEX IF NOT EXISTS ON {queue_table} (queue, priority) WHERE status = 0;
            CREATE INDEX IF NOT EXISTS ON {queue_table} (queue, status, update_time) WHERE status <> 0;

            CREATE TABLE IF NOT EXISTS {data_table} (
                id            uuid NOT NULL PRIMARY KEY,
                parent        uuid,
                depth         smallint,
                command       jsonb
            );
            CREATE INDEX IF NOT EXISTS ON {data_table} (parent, id) WHERE PARENT IS NOT NULL;
            """).format(
                queue_table=sql.Identifier(self._queue_table),
                data_table=sql.Identifier(self._data_table),
            ))

            cursor.connection.commit()

    def _push_jobs(self, jobs: Iterator[Job]) -> None:
        """ Adds a job (dictionary) to the database jobqueue table.
            Input:  credentials
                    group: str, name of "queue" in the database
                    job: dict, must be able to call json.dumps
            Output: None
        """
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        with CursorManager(self._credentials) as cursor:
            # cursor, insert_query, data, template=None, page_size=100
            command = sql.SQL("""
                WITH t AS (SELECT * FROM (VALUES %s) AS t (queue, priority, id, parent, depth, command),
                s AS (INSERT INTO {queue_table} (queue, priority, id)
                    (SELECT queue, priority, id FROM t)
                    ON CONFLICT DO NOTHING)
                INSERT INTO {data_table} (id, parent, depth, command)
                    (SELECT id, parent, depth, command FROM t)
                    ON CONFLICT DO NOTHING;""").format(
                queue_table=sql.Identifier(self._queue_table),
                data_table=sql.Identifier(self._data_table),
            )
            # TODO: remove queue id from here and put it into the command instead
            psycopg2.extras.execute_values(
                cursor,
                command,
                ((self._queue_id, j.priority, j.parent, j.depth, j.command)
                 for j in jobs),
                template=None,
                page_size=128,
            )

            cursor.connection.commit()

    def update_job(self, job_id: uuid.UUID) -> None:
        """ While a job is being worked on, the worker can periodically let the queue know it is still working on the
            job (instead of crashed or frozen).
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
                    UPDATE {queue_table}
                    SET update_time = NOW()
                    WHERE id = %s;
                    """).format(
                queue_table=sql.Identifier(self._queue_table)),
                [str(job_id)])

    def mark_job_complete(self, job_id: uuid.UUID) -> None:
        """ When a job is finished, this function will mark the status as done.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
                    UPDATE {queue_table}
                    SET status = 2,
                        update_time = NOW()
                    WHERE id = %s;
                    """).format(
                queue_table=sql.Identifier(self._queue_table)),
                [str(job_id)])

    def mark_job_failed(self, job_id: uuid.UUID, error: str) -> None:
        """ When a job failed, this function will mark the status as failed.
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL("""
                    UPDATE {queue_table}
                    SET status = -1,
                        update_time = NOW(),
                        error = %s,
                        error_count = COALESCE(error_count, 0) + 1
                    WHERE id = %s;
                    """).format(
                queue_table=sql.Identifier(self._queue_table)),
                [error, str(job_id)])

    def get_jobs_as_dataframe(self) -> pd.DataFrame:
        """ Returns all queues as a dataframe.
        """
        with CursorManager(self._credentials) as cursor:
            command = sql.SQL(
                """
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
                    d.quparenteue as parent,
                    d.depth as depth,
                    d.command as command
                FROM 
                    {queue_table} AS s,
                    {data_table} AS d
                WHERE
                    s.id = d.id;
                """).format(
                queue_table=sql.Identifier(self._queue_table),
                data_table=sql.Identifier(self._data_table))

            return pd.read_sql(command, con=cursor.connection)

    def get_queue_length(self) -> int:
        """ Returns the count of open jobs for a given group.
        Output: int, number of open jobs
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                SELECT COUNT(*) FROM {queue_table} 
                    WHERE 
                        queue = %s AND
                        status = 0;
                """
            ).format(
                queue_table=sql.Identifier(self._queue_table)),
                [self._queue_id])
            return cursor.fetchone()


# @property
# def messages(self):
#     res = [ x[1] for x in list_unprocessed(self._database) if x[0]==self._queue]
#     if len(res) > 0:
#         return res[0]
#     else:
#         return 0

# @property
# def all_messages(self):
#     res = [ x[1] for x in list_all(self._database) if x[0]==self._queue]
#     if len(res) > 0:
#         return res[0]
#     else:
#         return 0

# @property
# def average_time(self):
#     res = [ x[2] for x in list_time(self._database) if x[0]==self._queue]
#     if len(res) > 0:
#         return res[0]
#     else:
#         return 0
