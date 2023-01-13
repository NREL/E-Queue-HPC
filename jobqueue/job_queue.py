from functools import singledispatchmethod
import json
import random
import time
import traceback
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union
import uuid

import pandas as pd
import psycopg
from psycopg import sql

# import psycopg.extras

from jobqueue.job_status import JobStatus

from .job import Job
from .cursor_manager import CursorManager

# psycopg.extras.register_uuid()


class JobQueue:

    _credentials: Dict[str, Any]
    _queue_id: int

    _status_table: str
    _data_table: str

    def __init__(
        self,
        credentials: Dict[str, Any],
        queue: int = 0,
        check_table=False,
        drop_table=False,
    ) -> None:
        """Interface to the jobsque database table
        database: str, name of the key in your .jobqueue.json file.
        queue: str, name of the queue you'd like to create or use.
        _table_name is use for testing only.  To change your table name use the
        .jobqueue.json file.
        """

        credentials = credentials.copy()
        table_base_name = credentials["table_name"]
        del credentials["table_name"]

        self._credentials = credentials
        self._queue_id = queue
        self._status_table = table_base_name + "_status"
        self._data_table = table_base_name + "_data"

        # ensure table exists
        if check_table:
            self._create_tables(drop_table=drop_table)

    @property
    def queue_id(self) -> int:
        return self._queue_id

    def clear(self) -> None:
        """Clears all records for this queue."""
        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL("DELETE FROM {status_table} WHERE queue = %s;").format(
                    status_table=sql.Identifier(self._status_table)
                ),
                [self._queue_id],
            )

    def pop(
        self,
        n: Optional[int] = None,
        worker_id: Optional[uuid.UUID] = None,
    ) -> Union[List[Job], Optional[Job]]:
        """
        Claims and returns up to the requested number of jobs.
        An optional worker id can be assigned.

        If num_jobs is None (default), only one job will be popped and it will
        be returned bare. If the pop fails in this case, None will be returned
        instead of an empty list.
        """
        return self._pop(n, worker_id)

    @singledispatchmethod
    def _pop(
        self, n: Optional[int], worker_id: Optional[uuid.UUID]
    ) -> Union[List[Job], Optional[Job]]:
        raise TypeError()

    @_pop.register(type(None))
    def _(self, n: None, worker_id: Optional[uuid.UUID]) -> Optional[Job]:
        result = self._pop(1, worker_id)
        return None if len(result) == 0 else result[0]  # type: ignore

    @_pop.register(int)
    def _(self, n: int, worker_id: Optional[uuid.UUID]) -> List[Job]:
        if n <= 0:
            return []

        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
WITH p AS (
SELECT id, priority FROM {status_table}
    WHERE 
        queue = %(queue)s AND
        status = {queued_status}
    ORDER BY priority ASC
    LIMIT %(n)s FOR UPDATE SKIP LOCKED),
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
FROM p, {data_table} as t WHERE p.id = t.id;"""
                ).format(
                    status_table=sql.Identifier(self._status_table),
                    data_table=sql.Identifier(self._data_table),
                    queued_status=sql.Literal(JobStatus.Queued.value),
                    claimed_status=sql.Literal(JobStatus.Claimed.value),
                ),
                {
                    "queue": self._queue_id,
                    "worker_id": worker_id,
                    "n": n,
                },
            )
            result = [
                Job(
                    id=r[0],
                    status=JobStatus.Claimed,
                    priority=r[1],
                    command=r[2],
                )
                for r in cursor.fetchall()
            ]
        return result

    @singledispatchmethod
    def push(self, jobs: Iterator[Job]) -> None:
        """Adds jobs to the queue."""
        # https://stackoverflow.com/questions/8134602/psycopg-insert-multiple-rows-with-one-query
        with CursorManager(self._credentials) as cursor:
            # cursor, insert_query, data, template=None, page_size=100
            command = sql.SQL(
                """
WITH t AS (
SELECT 
    id::uuid,
    priority::int,
    command::jsonb
    FROM (VALUES %s) AS t (id, priority, command)),
v AS (INSERT INTO {status_table} (id, queue, priority) (SELECT id, {queue}, priority FROM t))
INSERT INTO {data_table} (id, command) (SELECT id, command FROM t);"""
            ).format(
                queue=sql.Literal(self._queue_id),
                status_table=sql.Identifier(self._status_table),
                data_table=sql.Identifier(self._data_table),
            )

            psycopg.extras.execute_values(
                cursor,
                command,
                (
                    (j.id, j.priority, json.dumps(j.command, separators=(",", ":")))
                    for j in jobs
                ),
                template=None,
                page_size=128,
            )

    @push.register(Job)
    def _(self, job: Job) -> None:
        """Adds a job to the queue."""
        self.push((job,))

    def update(self, job: Job) -> None:
        """While a job is being worked on, the worker can periodically let the queue know it is still working on the
        job (instead of crashed or frozen).
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
UPDATE {status_table}
SET update_time = NOW()
WHERE id = %s;"""
                ).format(status_table=sql.Identifier(self._status_table)),
                [job.id],
            )

    def complete(self, job: Job) -> None:
        """When a job is finished, this function will mark the status as done."""
        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
UPDATE {status_table}
SET status = {complete_status},
    update_time = NOW()
WHERE id = %s;"""
                ).format(
                    status_table=sql.Identifier(self._status_table),
                    complete_status=sql.Literal(JobStatus.Complete.value),
                ),
                [job.id],
            )

    def fail(self, job: Job, error: Optional[str]) -> None:
        """When a job failed, this function will mark the status as failed."""
        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
UPDATE {status_table}
SET status = {failed_status},
    update_time = NOW(),
    error = %s,
    error_count = COALESCE(error_count, 0) + 1
WHERE id = %s;"""
                ).format(
                    status_table=sql.Identifier(self._status_table),
                    failed_status=sql.Literal(JobStatus.Failed.value),
                ),
                [error, job.id],
            )

    def work_loop(
        self,
        handler: Callable[[uuid.UUID, Job], bool],
        worker_id: Optional[uuid.UUID] = None,
        wait_until_exit=15 * 60,
        maximum_waiting_time=5 * 60,
    ) -> None:
        print(f"Job Queue: Starting...", flush=True)

        worker_id = uuid.uuid4() if worker_id is None else worker_id
        wait_start = None
        wait_bound = 1.0
        continue_working: bool = True
        while continue_working:

            # Pull job off the queue
            job = self.pop(worker_id=worker_id)

            if job is None:

                if wait_start is None:
                    wait_start = time.time()
                    wait_bound = 1
                else:
                    waiting_time = time.time() - wait_start
                    if waiting_time > wait_until_exit:
                        print(
                            "Job Queue: No Jobs, max waiting time exceeded. Exiting...",
                            flush=True,
                        )
                        break

                # No jobs, wait and try again.
                print("Job Queue: No jobs found. Waiting...", flush=True)

                # bounded randomized exponential backoff
                wait_bound = min(maximum_waiting_time, wait_bound * 2)
                time.sleep(random.uniform(1.0, wait_bound))
                continue

            if not isinstance(job, Job):
                raise TypeError()  # should never happen

            try:
                wait_start = None

                print(f"Job Queue: {job.id} running...", flush=True)

                continue_working = handler(worker_id, job)  # handle the message

                # Mark the job as complete in the self.
                self.complete(job)

                print(f"Job Queue: {job.id} done.", flush=True)
            except Exception as e:
                print(
                    f"Job Queue: {job.id} unhandled exception {e} in work_loop.",
                    flush=True,
                )
                print(traceback.format_exc())
                try:
                    self.fail(job, str(e) + "\n" + traceback.format_exc())
                except Exception as e2:
                    print(
                        f"Job Queue: {job.id} exception thrown while marking as failed in work_loop: {e}, {e2}!",
                        flush=True,
                    )
                    print(traceback.format_exc(), flush=True)
        print(f"Job Queue: exiting work_loop.", flush=True)

    @property
    def message_counts(self) -> Tuple[int, int, int, int]:
        """Returns the count of open, pending, and completed jobs for a given group.
        Output: (num open, num pending, num completed)
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
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
            status = {failed_status}) AS failed;"""
                ).format(
                    status_table=sql.Identifier(self._status_table),
                    queued_status=sql.Literal(JobStatus.Queued.value),
                    claimed_status=sql.Literal(JobStatus.Claimed.value),
                    complete_status=sql.Literal(JobStatus.Complete.value),
                    failed_status=sql.Literal(JobStatus.Failed.value),
                ),
                [self._queue_id],
            )
            records = cursor.fetchone()
            return tuple(records)  # type: ignore

    def fail_incomplete_jobs(self, interval="12 hours") -> None:
        """Mark all incomplete jobs in a given group started more than {interval} ago as failed."""
        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
UPDATE {status_table}
    SET update_time = NOW()
    WHERE queue = %s
        AND status = {claimed_status}
        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;"""
                ).format(
                    status_table=sql.Identifier(self._status_table),
                    claimed_status=sql.Literal(JobStatus.Claimed.value),
                ),
                [self._queue_id, interval],
            )

    def reset_incomplete_jobs(self, interval="12 hours") -> None:
        """Mark all incomplete jobs in a given group started more than {interval} ago as open."""

        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
UPDATE {status_table}
    SET 
        status = {queued_status},
        start_time = NULL, 
        update_time = NULL, 
        error = NULL,
        error_count = COALESCE(error_count, 0) + 1
    WHERE queue = %s
        AND status = {claimed_status}
        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;"""
                ).format(
                    status_table=sql.Identifier(self._status_table),
                    queued_status=sql.Literal(JobStatus.Queued.value),
                    claimed_status=sql.Literal(JobStatus.Claimed.value),
                ),
                [self._queue_id, interval],
            )

    def reset_failed_jobs(self, interval="0 hours") -> None:
        """Mark all failed jobs in a given group started more than {interval} ago as open."""
        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
UPDATE {status_table}
    SET status = {queued_status},
        start_time = NULL, 
        update_time = NULL, 
        end_time = NULL, 
        error = NULL
    WHERE queue = %s
        AND status = {failed_status}
        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;"""
                ).format(
                    status_table=sql.Identifier(self._status_table),
                    queued_status=sql.Literal(JobStatus.Queued.value),
                    failed_status=sql.Literal(JobStatus.Failed.value),
                ),
                [self._queue_id, interval],
            )

    def _create_tables(self, drop_table: bool = False) -> None:
        """Deletes and replaces or creates the current jobqueue table."""
        with CursorManager(self._credentials) as cursor:
            with cursor.connection.transaction():
                if drop_table:
                    cursor.execute(
                        sql.SQL(
                            """
DROP TABLE IF EXISTS {status_table};
DROP TABLE IF EXISTS {data_table};"""
                        ).format(
                            status_table=sql.Identifier(self._status_table),
                            data_table=sql.Identifier(self._data_table),
                        )
                    )

                cursor.execute(
                    sql.SQL(
                        """
CREATE TABLE IF NOT EXISTS {status_table} (
    queue         smallint NOT NULL,
    status        smallint NOT NULL DEFAULT {queued_status},
    priority      int NOT NULL,
    id            uuid NOT NULL PRIMARY KEY,
    start_time    timestamp,
    update_time   timestamp,
    worker        uuid,
    error_count   smallint,
    error         text
);

CREATE INDEX IF NOT EXISTS {priority_index} ON {status_table} (queue, priority) WHERE status = {queued_status};

CREATE INDEX IF NOT EXISTS {update_index} ON {status_table} (queue, status, update_time) WHERE status > {queued_status};

CREATE TABLE IF NOT EXISTS {data_table} (
    id            uuid NOT NULL PRIMARY KEY,
    command       jsonb NOT NULL,
    parent        uuid
);

CREATE INDEX IF NOT EXISTS {parent_index} ON {data_table} (parent, id) WHERE parent IS NOT NULL;
"""
                    ).format(
                        status_table=sql.Identifier(self._status_table),
                        data_table=sql.Identifier(self._data_table),
                        priority_index=sql.Identifier(
                            self._status_table + "_priority_idx"
                        ),
                        update_index=sql.Identifier(self._status_table + "_update_idx"),
                        parent_index=sql.Identifier(self._data_table + "_parent_idx"),
                        queued_status=sql.Literal(JobStatus.Queued.value),
                    )
                )

    def get_jobs_as_dataframe(self) -> pd.DataFrame:
        """Returns all queues as a dataframe."""
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
    d.parent as parent,
    d.command as command
FROM 
    {status_table} AS s,
    {data_table} AS d
WHERE
    s.id = d.id AND
    queue = %(queue)s;"""
            ).format(
                status_table=sql.Identifier(self._status_table),
                data_table=sql.Identifier(self._data_table),
            )

            return pd.read_sql(
                command,  # type: ignore
                params={"queue": self._queue_id},  # type: ignore
                con=cursor.connection,
            )

    def get_queue_length(self) -> int:
        """Returns the count of open jobs for a given group.
        Output: int, number of open jobs
        """
        with CursorManager(self._credentials) as cursor:
            cursor.execute(
                sql.SQL(
                    """
SELECT COUNT(*) FROM {status_table} 
    WHERE 
        queue = %s AND
        status = {queued_status};"""
                ).format(
                    status_table=sql.Identifier(self._status_table),
                    queued_status=sql.Literal(JobStatus.Queued.value),
                ),
                [self._queue_id],
            )
            return cursor.fetchone()  # type: ignore
