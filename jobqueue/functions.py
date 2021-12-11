"""
Author: Monte Lunacek


    This file contains basic functions that execute SQL commands on a 
    database given a set credentials, for example:

    credentials = {
        "host": "yuma.hpc.nrel.gov",
        "user": "dmpappsops",
        "database": "dmpapps",
        "password": "*****************",
        "table_name": "jobqueue"
    }

    This can be passed to any function that has already created the jobsqueue 
    table.
"""
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

from jobqueue.cursor_manager import CursorManager
# from jobqueue.iter_file import IteratorFile
from jobqueue.job import Job
from jobqueue.job_queue import JobQueue
from jobqueue.job_status import JobStatus


def version(credentials: Dictionary[str, any]):
    """ Returns the current version of Postgres.
        Input: credentials
        Output: str with version information
    """

    record = None
    with CursorManager(credentials) as cursor:
        cursor.execute("SELECT version();")
        record = cursor.fetchone()

    return f"You are connected to - {record}"


def create_tables(queue: JobQueue, drop_table: bool = True) -> None:
    """ Deletes and replaces or creates the current jobqueue table.
    """

    with CursorManager(credentials, autocommit=False) as cursor:
        if drop_table:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(
                sql.Identifier(table_name)))

        cursor.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {status_table} (
            queue         int NOT NULL,
            priority      bigint NOT NULL,
            id            uuid NOT NULL PRIMARY KEY,
            run_count     smallint NOT NULL DEFAULT 0,
            start_time    timestamp,
            update_time   timestamp,
            end_time      timestamp,
            worker        uuid,
            error         text
        );
        CREATE INDEX IF NOT EXISTS ON {status_table} (queue, priority ASC) WHERE start_time IS NULL;
        CREATE INDEX IF NOT EXISTS ON {status_table} (queue, start_time) WHERE (start_time IS NOT NULL AND end_time IS NULL);
        CREATE INDEX IF NOT EXISTS ON {status_table} (queue, end_time) WHERE end_time IS NOT NULL;
        CREATE INDEX IF NOT EXISTS ON {status_table} (queue, error) WHERE error IS NOT NULL;
        CREATE TABLE IF NOT EXISTS {data_table} (
            id            uuid NOT NULL PRIMARY KEY,
            parent        uuid,
            depth         smallint NOT NULL,
            command       jsonb
        );
        CREATE INDEX IF NOT EXISTS ON {data_table} (parent, id);
        """).format(
            status_table=sql.Identifier(queue.status_table),
            data_table=sql.Identifier(queue.data_table),
        ))

        cursor.connection.commit()


def push_jobs(
    queue: JobQueue,
    jobs: Iterator[Job],
) -> None:
    """ Adds a job (dictionary) to the database jobqueue table.
        Input:  credentials
                group: str, name of "queue" in the database
                job: dict, must be able to call json.dumps
        Output: None
    """
    # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
    with CursorManager(queue.credentials, autocommit=True) as cursor:
        # cursor, insert_query, data, template=None, page_size=100
        command = sql.SQL("""
            WITH t AS (SELECT * FROM (VALUES %s) AS t (queue, priority, id, parent, depth, command),
            s AS (INSERT INTO {status_table} (queue, priority, id)
                (SELECT queue, priority, id FROM t)
                ON CONFLICT DO NOTHING
            INSERT INTO {data_table} (id, parent, depth, command)
                (SELECT id, parent, depth, command FROM t)
                ON CONFLICT DO NOTHING;""").format(
            status_table=sql.Identifier(queue.status_table),
            data_table=sql.Identifier(queue.data_table),
        )
        # TODO: remove queue id from here and put it into the command instead
        psycopg2.extras.execute_values(
            cursor,
            command,
            ((queue.queue_id, j.queue, j.priority, j.parent, j.depth, j.command)
             for j in jobs),
            template=None,
            page_size=128,
        )

        cursor.connection.commit()


def pop_jobs(queue: JobQueue, worker_id: Optional[uuid.UUID], num_jobs: int = 1) -> any:
    """ 
    Claims and returns jobs from the queue.  
    An optional worker id can be assigned. 
    """

    host = platform.node()

    with CursorManager(queue.credentials) as cursor:
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
        command = sql.SQL("""
        WITH p AS (
            SELECT id, priority FROM {queue_table}
                WHERE 
                    queue = %(queue)s AND
                    start_time IS NULL
                ORDER BY priority ASC
                LIMIT %(num_jobs)s FOR UPDATE SKIP LOCKED),
        u AS (
            UPDATE {queue_table} as q
                SET run_count = q.run_count + 1,
                    worker = %(worker_id)s,
                    start_time = NOW(),
                    update_time = NOW()
                FROM p
                WHERE q.id = q.id)
        SELECT popped.id AS id, popped.priority AS priority, task.config AS config
        FROM
            popped,
            {task_table} as task
        WHERE
            popped.id = task.id;""").format(
            queue_table=sql.Identifier(table_name),
            task_table=sql.Identifier(table_name),
        )
        cursor.execute(command, {
            'queue': queue.queue_id,
            'worker_id': worker_id,
            'num_jobs': num_jobs,
        })

        # command = sql.SQL("""
        # WITH popped AS (
        #     DELETE FROM {queue_table}
        #         WHERE id IN (
        #             SELECT id FROM {queue_table}
        #                 WHERE queue = %(queue)s
        #                 ORDER BY priority ASC
        #                 LIMIT %(num)s FOR UPDATE SKIP LOCKED)
        #         RETURNING id, priority),
        #     run_record AS (
        #         INSERT INTO {status_table} (id, %(queue)s, %(worker)s, NOW(), NOW(), NULL)
        #             SELECT id FROM popped
        #             RETURNING id
        #     )
        # SELECT popped.id AS id, popped.priority AS priority, task.config
        # FROM
        #     popped,
        #     {task_table} as task
        # WHERE
        #     popped.id = task.id;
        # """).format(
        #     queue_table = sql.Identifier(table_name),
        #     status_table = sql.Identifier(table_name))
        # cursor.execute(command, {
        #     'queue':queue,
        #     'num':num,
        #     'worker':worker,
        #     })

        # SELECT
        #     id, config, priority
        # FROM {task_table} as tt
        #     WHERE id IN (
        #         DELETE FROM {queue_table}
        #         WHERE id IN (
        #             SELECT id FROM {queue_table}
        #             WHERE queue = %s
        #             ORDER BY priority ASC, RANDOM()
        #             LIMIT %s FOR UPDATE SKIP LOCKED
        #             )
        #         RETURNING id, priority)
        #     """).format(
        #         task_table = sql.Identifier(task_table),
        #         queue_table = sql.Identifier(queue_table))
        # command = sql.SQL("""
        #                     UPDATE {queue_table}
        #                     SET
        #                         priority = NULL,
        #                         worker = %(worker)s,
        #                         start_time = CURRENT_TIMESTAMP,
        #                         update_time = CURRENT_TIMESTAMP
        #                     WHERE id = (SELECT id
        #                                   FROM {queue_table}
        #                                   WHERE
        #                                     priority IS NOT NULL
        #                                     AND queue = %(queue)s
        #                                   ORDER BY priority ASC, RANDOM()
        #                                   LIMIT 1 FOR UPDATE SKIP LOCKED)
        #                     RETURNING id, config, priority;
        #                     """).format(sql.Identifier(table_name),
        #                           sql.Identifier(table_name))
        # cursor.execute(command, [host, None if worker is None else str(worker), group])

        # command = sql.SQL("""
        #                     UPDATE {}
        #                     SET status = 'running',
        #                         host = %s,
        #                         worker = %s,
        #                         start_time = CURRENT_TIMESTAMP,
        #                         update_time = CURRENT_TIMESTAMP
        #                     WHERE uuid = (SELECT uuid
        #                                   FROM {}
        #                                   WHERE status IS NULL
        #                                     AND groupname = %s
        #                                   ORDER BY priority ASC, RANDOM()
        #                                   LIMIT 1 FOR UPDATE SKIP LOCKED)
        #                     RETURNING uuid, config, priority;
        #                     """).format(sql.Identifier(table_name),
        #                           sql.Identifier(table_name))
        if num_jobs == 1:
            return cursor.fetchone()
        return cursor.fetchall()


def update_job(queue: JobQueue, job_id: uuid.UUID) -> None:
    """ While a job is being worked on, the worker can periodically let the queue know it is still working on the
        job (instead of crashed or frozen).
    """
    with CursorManager(queue.credentials) as cursor:
        cursor.execute(sql.SQL("""
                UPDATE {status_table}
                SET update_time = NOW()
                WHERE id = %s;
                """).format(
            status_table=sql.Identifier(queue.status_table)),
            [str(job_id)])


def mark_job_complete(queue: JobQueue, job_id: uuid.UUID) -> None:
    """ When a job is finished, this function will mark the status as done.
    """
    with CursorManager(queue.credentials) as cursor:
        cursor.execute(sql.SQL("""
                UPDATE {status_table}
                SET update_time = NOW(),
                    end_time = NOW()
                WHERE id = %s;
                """).format(
            status_table=sql.Identifier(queue.status_table)),
            [str(job_id)])


def mark_job_failed(queue: JobQueue, job_id: uuid.UUID, error: str) -> None:
    """ When a job failed, this function will mark the status as failed.
    """
    with CursorManager(queue.credentials) as cursor:
        cursor.execute(sql.SQL("""
                UPDATE {status_table}
                SET update_time = NOW(),
                    end_time = NOW(),
                    error = %s
                WHERE id = %s;
                """).format(
            status_table=sql.Identifier(queue.status_table)),
            [error, str(job_id)])


def clear_queue(queue: JobQueue) -> None:
    """ Clears all records for a given group.
    """
    with CursorManager(queue.credentials) as cursor:
        cursor.execute(sql.SQL("DELETE FROM {status_table} WHERE queue = %s;").format(
            status_table=sql.Identifier(queue.status_table)),
            [queue.queue_id])


def clear_all_queues(queue: JobQueue) -> None:
    """ Clears all records in the table.
    """
    with CursorManager(queue.credentials) as cursor:
        cursor.execute(sql.SQL("DELETE FROM {status_table};").format(
            status_table=sql.Identifier(queue.status_table)),
            [queue.queue_id])


def get_jobs_as_dataframe(queue: JobQueue) -> pd.DataFrame:
    """ Returns all queues as a dataframe.
    """
    with CursorManager(queue.credentials) as cursor:
        command = sql.SQL(
            """
            SELECT 
                s.queue as queue,
                s.priority as priority,
                s.id as id,
                s.run_count as run_count,
                s.start_time as start_time,
                s.update_time as update_time,
                s.end_time as end_time,
                s.worker as worker,
                s.error as error,
                d.quparenteue as parent,
                d.depth as depth,
                d.command as command
            FROM 
                {status_table} AS s,
                {data_table} AS d
            WHERE
                s.id = d.id;
            """).format(
            status_table=sql.Identifier(queue.status_table))
    
        return pd.read_sql(command, con=cursor.connection)


def get_queue_length(queue: JobQueue) -> int:
    """ Returns the count of open jobs for a given group.
    Output: int, number of open jobs
    """
    with CursorManager(queue.credentials) as cursor:
        cursor.execute(sql.SQL(
            """
            SELECT COUNT(*) FROM {status_table} 
                WHERE 
                    queue = %s AND
                    priority IS NOT NULL;
            """
        ).format(
            status_table=sql.Identifier(queue.status_table)),
            [queue.queue_id])
        return cursor.fetchone()


def get_job_counts(queue: JobQueue) -> Tuple[int, int, int]:
    """ Returns the count of open, pending, and completed jobs for a given group.
   Output: (num open, num pending, num completed)
   """
    with CursorManager(queue.credentials) as cursor:
        cursor.execute(sql.SQL(
            """
            SELECT 
                (SELECT COUNT(*) FROM {status_table} 
                    WHERE queue = %s AND
                        start_time IS NULL) AS open,
                (SELECT COUNT(*) FROM {status_table} 
                    WHERE queue = %s AND
                        start_time IS NOT NULL AND end_time IS NULL) AS running,
                (SELECT COUNT(*) FROM {status_table} 
                    WHERE queue = %s AND
                        start_time IS NOT NULL AND end_time IS NOT NULL) AS done
            ;
            """
        ).format(
            status_table=sql.Identifier(queue.status_table)),
            [queue.queue_id])
        records = cursor.fetchone()
        return tuple(records)


def fail_incomplete_jobs(queue: JobQueue, interval='4 hours') -> None:
    """ Mark all incomplete jobs in a given group started more than {interval} ago as failed.
    """
    with CursorManager(queue.credentials) as cursor:
        cursor.execute(sql.SQL(
            """
            UPDATE {status_table}
                SET error = 'timed out',
                end_time = NOw()
                WHERE queue = %s
                    AND start_time IS NOT NULL AND end_time IS NULL
                    AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;
            """
        ).format(
            status_table=sql.Identifier(queue.status_table)),
            [queue.queue_id, interval])


def reset_incomplete_jobs(queue: JobQueue,interval='4 hours') -> None:
    """ Mark all incomplete jobs in a given group started more than {interval} ago as open.
    """

    with CursorManager(credentials) as cursor:
        cursor.execute(sql.SQL(
            """
            UPDATE {status_table}
                SET start_time = NULL, update_time = NULL, 
                    run_count = run_count + 1
                WHERE queue = %s
                    AND start_time IS NOT NULL AND end_time IS NULL
                    AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;
            """
        ).format(
            status_table=sql.Identifier(queue.status_table)),
            [queue.queue_id, interval])


def reset_failed_jobs(
        queue: JobQueue,
        interval='4 hours',
) -> None:
    """ Mark all failed jobs in a given group started more than {interval} ago as open.
    """

    with CursorManager(credentials) as cursor:
        command = """
                UPDATE {}
                SET start_time = NULL, update_time = NULL, end_time = NULL, error = NULL
                WHERE groupname = %s AND 
                    error IS NOT NULL AND 
                    start_time IS NOT NULL AND
                    end_time IS NOT NULL;
                """
        command = sql.SQL(command).format(sql.Identifier(table_name))
        cursor.execute(command, [group, interval])
