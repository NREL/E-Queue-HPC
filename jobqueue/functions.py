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

import os
import sys
import json
import copy
import pandas as pd
import psycopg2
from psycopg2 import sql
import uuid
import datetime
import time
import socket
import platform
from typing import Callable, Optional


def execute_database_command(credentials: {str, any}, execution_function: Callable[[any], any]) -> any:
    """ Executes a function inside the scope of a database cursor, and cleans up and catches database exceptions while
        executing that function.
        Input: credentials
               execution_function - function to which the cursor is passed
        Output: return value of execution_function() call
    """
    connection = psycopg2.connect(**credentials)

    cursor = None
    result = None
    try:
        cursor = connection.cursor()
        result = execution_function(cursor)
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
        raise error
    finally:
        if connection is not None:
            if cursor is not None:
                cursor.close()
            connection.close()
    return result


def version(credentials: {str: any}):
    """ Returns the current version of Postgres.
        Input: credentials
        Output: str with version information
    """

    def command(cursor):
        cursor.execute("SELECT version();")
        return cursor.fetchone()

    record = execute_database_command(credentials, command)
    return f"You are connected to - {record}"


def create_table(credentials: {str: any}, table_name: str):
    """ Creates the table if it does not exists
        Input: credentials
        Output: None
    """
    recreate_table(credentials, table_name, drop_table=False)


def recreate_table(credentials: {str: any}, table_name: str, drop_table: bool = True) -> None:
    """ Deletes and replaces or creates the current jobqueue table.
    """

    def command(cursor):
        if drop_table:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(table_name)))

        cmd = """
        CREATE TABLE IF NOT EXISTS {} (
            UUID            UUID NOT NULL PRIMARY KEY,
            username        VARCHAR NOT NULL,
            config          JSON    NOT NULL,
            groupname       VARCHAR,
            host            VARCHAR,
            status          VARCHAR,
            worker          UUID,
            creation_time   TIMESTAMP,
            priority        VARCHAR,
            start_time      TIMESTAMP,
            update_time     TIMESTAMP,
            end_time        TIMESTAMP,
            depth         INTEGER,
            wall_time       FLOAT
        );
        CREATE INDEX IF NOT EXISTS {} ON {} (groupname, priority ASC) WHERE status IS NULL;
        CREATE INDEX IF NOT EXISTS {} ON {} (groupname, status);
        """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name),
                                  sql.Identifier(table_name + "_groupname_null_status_priority_idx"),
                                  sql.Identifier(table_name),
                                  sql.Identifier(table_name + "_groupname_status_idx"),
                                  sql.Identifier(table_name),
                                  )
        cursor.execute(cmd)

    execute_database_command(credentials, command)


def add_job(
        credentials: {str: any},
        table_name: str,
        group: str,
        job: dict,
        priority: Optional[str] = None,
) -> None:
    """ Adds a job (dictionary) to the database jobqueue table.
        Input:  credentials
                group: str, name of "queue" in the database
                job: dict, must be able to call json.dumps
        Output: None
    """

    def command(cursor):
        nonlocal priority
        job_id = job.get('uuid', str(uuid.uuid4()))
        user = os.environ.get('USER')
        if priority is None:
            priority = str(datetime.datetime.now())

        cmd = sql.SQL("""
                    INSERT INTO {}(uuid, username, config, groupname, 
                                         host, status, worker, creation_time, priority) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)""").format(sql.Identifier(table_name))
        args = (job_id, user, json.dumps(job), group, None, None, None, priority)
        cursor.execute(cmd, args)

    execute_database_command(credentials, command)


def fetch_job(
        credentials: {str: any},
        table_name: str,
        group: str,
        worker: Optional[uuid.UUID] = None,
) -> any:
    """ Gets an available job from the group (queue, experiment, etc.).  An optional
        worker id can be assigned.  After the job is allocated to the function,
        several job characteristics are updated.

        Input:  credentials
                table_name
                group: str, name of groupname in table
                worker: uuid.UUID, id for worker
        Output: A job record from jobqueue table
    """

    host = platform.node()

    def command(cursor):
        tic = time.time()
        cmd = """
                            UPDATE {}
                            SET status = 'running',
                                host = %s,
                                worker = %s,
                                start_time = CURRENT_TIMESTAMP,
                                update_time = CURRENT_TIMESTAMP
                            WHERE uuid = (SELECT uuid 
                                          FROM {} 
                                          WHERE status IS NULL  
                                            AND groupname = %s
                                          ORDER BY priority ASC, RANDOM()
                                          LIMIT 1 FOR UPDATE)
                            RETURNING *
                            """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name),
                                  sql.Identifier(table_name))
        cursor.execute(cmd, [host, worker, group])
        return cursor.fetchone()

    return execute_database_command(credentials, command)


def update_job_status(credentials: {str: any}, table_name: str, uuid: uuid.UUID) -> None:
    """ While a job is being worked on, the worker can periodically let the queue know it is still working on the
        job (instead of crashed or frozen).

        Input:  credentials
                table_name
                uuid, str: the id of the job in the table

        Output: None
    """

    def command(cursor):
        cmd = sql.SQL(
            """
            UPDATE {}
            SET update_time = CURRENT_TIMESTAMP
            WHERE uuid = %s
            """).format(sql.Identifier(table_name))
        cursor.execute(cmd, [str(uuid)])

    execute_database_command(credentials, command)


def mark_job_as_done(credentials: {str: any}, table_name: str, uuid: uuid.UUID) -> None:
    """ When a job is finished, this function will mark the status as done.

        Input:  credentials
                table_name
                uuid, str: the id of the job in the table

        Output: None
    """

    def command(cursor):
        cmd = sql.SQL("""
                UPDATE {}
                SET status = 'done',
                    update_time = CURRENT_TIMESTAMP,
                    end_time = CURRENT_TIMESTAMP 
                WHERE uuid = %s
                """).format(sql.Identifier(table_name))
        cursor.execute(cmd, [str(uuid)])

    execute_database_command(credentials, command)


def clear_queue(credentials: {str: any}, table_name: str, groupname: str) -> None:
    """ Clears all records for a given group.

        Input: credentials
                table_name
                groupname, str: Name of group (queue)
        Output: None
    """

    def command(cursor):
        cmd = """
                   DELETE FROM {} WHERE groupname = %s;    
                   """
        data = [groupname]
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        cursor.execute(cmd, data)

    execute_database_command(credentials, command)


def clear_table(credentials: {str: any}, table_name: str) -> None:
    """ Clears all records in the table.
        Input: credentials
                table_name
        Output: None
    """

    def command(cursor):
        cursor.execute(sql.SQL('DELETE FROM {};').format(sql.Identifier(table_name)))

    execute_database_command(credentials, command)


def get_dataframe(credentials: {str: any}, table_name: str) -> pd.DataFrame:
    """ Returns the entire table as a dataframe.
        Input: credentials
                table_name
        Output: pandas.DataFrame
    """
    connection = psycopg2.connect(**credentials)
    cmd = sql.SQL('SELECT * FROM {}').format(sql.Identifier(table_name))
    df = pd.read_sql(cmd, con=connection)
    return df


def get_messages(credentials: {str: any}, table_name: str, group: str) -> int:
    """ Returns the count of open jobs for a given group.
    Input: credentials, table_name, group
    Output: int, number of open jobs
    """
    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        cmd = """SELECT COUNT(*) FROM {} 
                 WHERE groupname = %s AND
                    status IS NULL;    
              """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        data = [group]
        cursor.execute(cmd, data)
        return cursor.fetchone()

    return execute_database_command(credentials, command)


def get_message_counts(credentials: {str: any}, table_name: str, group: str) -> (int, int, int):
    """ Returns the count of open, pending, and completed jobs for a given group.
   Input: credentials, table_name, group
   Output: (num open, num pending, num completed)
   """

    def command(cursor):
        cursor.execute(sql.SQL("""
                        SELECT status, COUNT(*) FROM {}
                        WHERE groupname = %s
                        GROUP BY status;
                       """).format(sql.Identifier(table_name)),
                       [group])
        return cursor.fetchall()

    records = {r[0]: r[1] for r in execute_database_command(credentials, command)}
    return records.get(None, 0), records.get(None, 'running'), records.get(None, 'done')


def reset_incomplete_jobs(credentials: {str: any}, table_name: str, group: str, interval='0 hours') -> None:
    """ Mark all incomplete jobs in a given group started more than {interval} ago as open.

        Input:  credentials
                table_name
                group, str: the group name
                interval, str: Postgres interval string specifying time from now, before which jobs will be reset

        Output: None
    """

    def command(cursor):
        cmd = """
                UPDATE {}
                SET status = null,
                    start_time = null,
                    host = null,
                    aquire = null
                WHERE groupname = %s
                    and status = 'running'
                    and update_time < CURRENT_TIMESTAMP - INTERVAL %s;
                """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        cursor.execute(cmd, [group, interval])

    execute_database_command(credentials, command)
