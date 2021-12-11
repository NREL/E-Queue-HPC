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

from jobqueue.cursor_manager import CursorManager

from . import functions
from .job import Job


def __init__(self, database: str, queue: int = 0, table_base_name: str = 'job', pooling=False, check_table=False) -> None:
    """ Interface to the jobsque database table
        database: str, name of the key in your .jobsqueue.json file.
        queue: str, name of the queue you'd like to create or use.
        _table_name is use for testing only.  To change your table name use the
        .jobsqueue.json file.
    """

    _database: str
    queue_id: int
    # self.queue_table: str = _table_name + '_queue'
    queue_table: str
    data_table: str

    def __init__(self) -> None:

        self._database = database
        self.queue_id = queue
        # self.queue_table: str = _table_name + '_queue'
        self.queue_table = table_base_name + '_status'
        self.data_table = table_base_name + '_data'

        filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
        try:
            _data = json.loads(open(filename).read())
            self.credentials = _data[self._database].copy()
            if pooling:
                self.credentials['pooling'] = pooling
            if 'table_name' in self.credentials:
                del self.credentials['table_name']

        except KeyError as e:
            raise Exception(
                "No credentials for {} found in {}".format(database, filename))

        if check_table:
            # ensure table exists
            functions.create_tables(self.credentials, self._table_name)

    @property
    def messages(self) -> int:
        res = functions.get_queue_length(
            self.credentials, self._table_name, self._queue)
        return res[0]

    @property
    def message_counts(self) -> Tuple[int, int, int]:
        return functions.get_job_counts(self)

    def clear(self) -> None:
        functions.clear_queue(self)

    def get_message(self, worker: Optional[uuid.UUID] = None) -> Optional[Job]:
        res = functions.pop_jobs(
            self.credentials, self._table_name, self._queue, worker=worker)
        if res is None:
            return None
        return Job(self, result[0], result[1], command=result[2])

    def add_job(self, job, priority=None) -> None:
        functions.add_job(self.credentials, self._table_name,
                          self._queue, job, priority=priority)

    def fail_incomplete_jobs(self, interval='4 hours') -> None:
        functions.fail_incomplete_jobs(
            self.credentials, self._table_name, self._queue, interval=interval)

    def reset_incomplete_jobs(self, interval='4 hours') -> None:
        functions.reset_incomplete_jobs(
            self.credentials, self._table_name, self._queue, interval=interval)

    def reset_failed_jobs(self) -> None:
        functions.reset_failed_jobs(
            self.credentials, self._table_name, self._queue)

    def run_worker(self, handler: Callable[[uuid.UUID, Job], None], wait_until_exit=15 * 60,
                   maximum_waiting_time=5 * 60):
        print(f"Job Queue: Starting...")

        worker_id = uuid.uuid4()
        wait_start = None
        wait_bound = 1.0
        while True:

            # Pull job off the queue
            message = self.get_message(worker=worker_id)

            if message is None:

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

            try:
                wait_start = None
                print(f"Job Queue: {message.uuid} running...")

                handler(worker_id, message)  # handle the message
                # Mark the job as complete in the queue.
                message.mark_complete()

                print(f"Job Queue: {message.uuid} done.")
            except Exception as e:
                print(
                    f"Job Queue: {message.uuid} unhandled exception {e} in jq_runner.")
                print(traceback.format_exc())
                try:
                    message.mark_failed()
                except Exception as e2:
                    print(
                        f"Job Queue: {message.uuid} exception thrown while marking as failed in jq_runner: {e}, {e2}!")
                    print(traceback.format_exc())

    def _create_tables(self, drop_table: bool = True) -> None:
        """ Deletes and replaces or creates the current jobqueue table.
        """
        with CursorManager(self.credentials, autocommit=False) as cursor:
            if drop_table:
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(
                    sql.Identifier(table_name)))

            cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {queue_table} (
                queue         int NOT NULL,
                priority      bigint NOT NULL,
                id            uuid NOT NULL PRIMARY KEY,
                run_count     smallint NOT NULL DEFAULT 0,
                start_time    timestamp,
                update_time   timestamp,
                worker        uuid,
                error         text
            );
            CREATE INDEX IF NOT EXISTS ON {queue_table} (queue, priority) WHERE start_time IS NULL;
            CREATE INDEX IF NOT EXISTS ON {queue_table} (queue, status, update_time) WHERE (start_time IS NOT NULL);

            CREATE INDEX IF NOT EXISTS ON {queue_table} (queue, update_time) WHERE (start_time IS NOT NULL AND end_time IS NULL);
            CREATE INDEX IF NOT EXISTS ON {queue_table} (queue, end_time) WHERE end_time IS NOT NULL;
            CREATE INDEX IF NOT EXISTS ON {queue_table} (queue, end_time) WHERE error IS NOT NULL;
            CREATE TABLE IF NOT EXISTS {data_table} (
                id            uuid NOT NULL PRIMARY KEY,
                parent        uuid,
                depth         smallint NOT NULL,
                command       jsonb
            );
            CREATE INDEX IF NOT EXISTS ON {data_table} (parent, id);
            """).format(
                queue_table=sql.Identifier(self.queue_table),
                data_table=sql.Identifier(queue.data_table),
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
        with CursorManager(self.credentials, autocommit=True) as cursor:
            # cursor, insert_query, data, template=None, page_size=100
            command = sql.SQL("""
                WITH t AS (SELECT * FROM (VALUES %s) AS t (queue, priority, id, parent, depth, command),
                s AS (INSERT INTO {queue_table} (queue, priority, id)
                    (SELECT queue, priority, id FROM t)
                    ON CONFLICT DO NOTHING)
                INSERT INTO {data_table} (id, parent, depth, command)
                    (SELECT id, parent, depth, command FROM t)
                    ON CONFLICT DO NOTHING;""").format(
                queue_table=sql.Identifier(self.queue_table),
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


    def _pop_jobs(self, worker_id: Optional[uuid.UUID], num_jobs: int = 1) -> any:
        """ 
        Claims and returns jobs from the queue.  
        An optional worker id can be assigned. 
        """

        host = platform.node()

        with CursorManager(self.credentials) as cursor:
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
                        start_time IS NULL
                    ORDER BY priority ASC
                    LIMIT %(num_jobs)s FOR UPDATE SKIP LOCKED),
            u AS (
                UPDATE {queue_table} as q
                    SET run_count = run_count + 1,
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
                queue_table=sql.Identifier(self.queue_table),
                data_table=sql.Identifier(self.data_Table),
            ) {
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
            #         INSERT INTO {queue_table} (id, %(queue)s, %(worker)s, NOW(), NOW(), NULL)
            #             SELECT id FROM popped
            #             RETURNING id
            #     )
            # SELECT popped.id AS id, popped.priority AS priority, task.config
            # FROM
            #     popped,
            #     {data_table} as task
            # WHERE
            #     popped.id = task.id;
            # """).format(
            #     queue_table = sql.Identifier(table_name),
            #     queue_table = sql.Identifier(table_name))
            # cursor.execute(command, {
            #     'queue':queue,
            #     'num':num,
            #     'worker':worker,
            #     })

            # SELECT
            #     id, config, priority
            # FROM {data_table} as tt
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
            #         data_table = sql.Identifier(data_table),
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


    def _update_job(self, job_id: uuid.UUID) -> None:
        """ While a job is being worked on, the worker can periodically let the queue know it is still working on the
            job (instead of crashed or frozen).
        """
        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL("""
                    UPDATE {queue_table}
                    SET update_time = NOW()
                    WHERE id = %s;
                    """).format(
                queue_table=sql.Identifier(self.queue_table)),
                [str(job_id)])


    def _mark_job_complete(self, job_id: uuid.UUID) -> None:
        """ When a job is finished, this function will mark the status as done.
        """
        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL("""
                    UPDATE {queue_table}
                    SET update_time = NULL,
                        end_time = NOW()
                    WHERE id = %s;
                    """).format(
                queue_table=sql.Identifier(self.queue_table)),
                [str(job_id)])


    def mark_job_failed(self, job_id: uuid.UUID, error: str) -> None:
        """ When a job failed, this function will mark the status as failed.
        """
        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL("""
                    UPDATE {queue_table}
                    SET update_time = NOW(),
                        end_time = NOW(),
                        error = %s
                    WHERE id = %s;
                    """).format(
                queue_table=sql.Identifier(self.queue_table)),
                [error, str(job_id)])


    def clear_queue(self) -> None:
        """ Clears all records for a given group.
        """
        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL("DELETE FROM {queue_table} WHERE queue = %s;").format(
                queue_table=sql.Identifier(self.queue_table)),
                [queue.queue_id])


    def clear_all_queues(self) -> None:
        """ Clears all records in the table.
        """
        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL("DELETE FROM {queue_table};").format(
                queue_table=sql.Identifier(self.queue_table)),
                [queue.queue_id])


    def get_jobs_as_dataframe(self) -> pd.DataFrame:
        """ Returns all queues as a dataframe.
        """
        with CursorManager(self.credentials) as cursor:
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
                    {queue_table} AS s,
                    {data_table} AS d
                WHERE
                    s.id = d.id;
                """).format(
                queue_table=sql.Identifier(self.queue_table),
                data_table=sql.Identifier(self.data_table))
        
            return pd.read_sql(command, con=cursor.connection)


    def get_queue_length(self) -> int:
        """ Returns the count of open jobs for a given group.
        Output: int, number of open jobs
        """
        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                SELECT COUNT(*) FROM {queue_table} 
                    WHERE 
                        queue = %s AND
                        priority IS NOT NULL;
                """
            ).format(
                queue_table=sql.Identifier(self.queue_table)),
                [queue.queue_id])
            return cursor.fetchone()


    def get_job_counts(self) -> Tuple[int, int, int]:
        """ Returns the count of open, pending, and completed jobs for a given group.
    Output: (num open, num pending, num completed)
    """
        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                SELECT 
                    (SELECT COUNT(*) FROM {queue_table} 
                        WHERE queue = %s AND
                            start_time IS NULL) AS open,
                    (SELECT COUNT(*) FROM {queue_table} 
                        WHERE queue = %s AND
                            start_time IS NOT NULL AND end_time IS NULL) AS running,
                    (SELECT COUNT(*) FROM {queue_table} 
                        WHERE queue = %s AND
                            start_time IS NOT NULL AND end_time IS NOT NULL) AS done
                ;
                """
            ).format(
                queue_table=sql.Identifier(self.queue_table)),
                [queue.queue_id])
            records = cursor.fetchone()
            return tuple(records)


    def fail_incomplete_jobs(self, interval='4 hours') -> None:
        """ Mark all incomplete jobs in a given group started more than {interval} ago as failed.
        """
        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                UPDATE {queue_table}
                    SET error = 'timed out',
                        end_time = NOw()
                    WHERE queue = %s
                        AND start_time IS NOT NULL 
                        AND end_time IS NULL
                        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;
                """
            ).format(
                queue_table=sql.Identifier(self.queue_table)),
                [queue.queue_id, interval])


    def reset_incomplete_jobs(self,interval='4 hours') -> None:
        """ Mark all incomplete jobs in a given group started more than {interval} ago as open.
        """

        with CursorManager(self.credentials) as cursor:
            cursor.execute(sql.SQL(
                """
                UPDATE {queue_table}
                    SET 
                        start_time = NULL, 
                        update_time = NULL, 
                        run_count = run_count + 1
                    WHERE queue = %s
                        AND start_time IS NOT NULL 
                        AND end_time IS NULL
                        AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;
                """
            ).format(
                queue_table=sql.Identifier(self.queue_table)),
                [queue.queue_id, interval])


    def reset_failed_jobs(
            self,
            interval='4 hours',
    ) -> None:
        """ Mark all failed jobs in a given group started more than {interval} ago as open.
        """
        with CursorManager(self.credentials) as cursor:
                    cursor.execute(sql.SQL(
                        """
                        UPDATE {queue_table}
                            SET start_time = NULL, 
                                update_time = NULL, 
                                end_time = NULL, 
                                error = NULL,
                                run_count = run_count + 1
                            WHERE queue = %s
                                AND start_time IS NOT NULL 
                                AND end_time IS NOT NULL 
                                AND error IS NOT NULL
                                AND update_time < CURRENT_TIMESTAMP - INTERVAL %s;
                        """
                    ).format(
                        queue_table=sql.Identifier(self.queue_table)),
                        [queue.queue_id, interval])



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
