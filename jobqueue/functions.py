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


def execute_database_command(credentials, execution_function) -> any:
    """ Executes a function inside the scope of a database cursor, and cleans up and catches database exceptions while
        executing that function.
        Input: credentials
               execution_function - function to which the cursor is passed
        Output: None
    """
    table_name, credentials = get_table_name(credentials)
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
    finally:
        if connection and cursor is not None:
            cursor.close()
    connection.close()
    return result


def get_table_name(credentials):
    """ The input credentials may contain a table_name.  This function
        returns the table name and a workable version of credentials.
    """
    tmp = copy.deepcopy(credentials)
    table_name = tmp.get("table_name", "jobqueue")
    if "table_name" in tmp.keys():
        del tmp['table_name']
    return table_name, tmp


def version(credentials):
    """ Returns the current version of Postgres.
        Input: credentials
        Output: str with version information
    """

    def command(cursor):
        cursor.execute("SELECT version();")
        return cursor.fetchone()

    record = execute_database_command(credentials, command)
    return f"You are connected to - {record}"


def create_table(credentials):
    """ Creates the table if it does not exists
        Input: credentials
        Output: None
    """
    recreate_table(credentials, drop_table=False)


def recreate_table(credentials, drop_table=True):
    """ Deletes and replaces or creates the current jobqueue table.
        Input: credentials
        Output: None
    """
    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        if drop_table:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(table_name)))

        cmd = """
        CREATE TABLE {} (
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
            `depth`          INTEGER,
            wall_time       FLOAT,
            aquire          FLOAT,
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


def add_job(credentials, group, job, priority=None):
    """ Adds a job (dictionary) to the database jobqueue table.
        Input:  credentials
                group: str, name of "queue" in the database
                job: dict, must be able to call json.dumps
        Output: None
    """
    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        job_id = job.get('uuid', str(uuid.uuid4()))
        user = os.environ.get('USER')
        if priority is None:
            priority = str(datetime.datetime.now())

        cmd = sql.SQL("""
                    INSERT INTO {}(uuid, username, config, groupname, 
                                         host, status, worker, creation_time, priority) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, current_timestamp, %s)""").format(sql.Identifier(table_name))
        args = (job_id, user, json.dumps(job), group, None, None, None, priority)
        cursor.execute(cmd, args)

    execute_database_command(credentials, command)


def fetch_job(credentials, group, worker=None):
    """ Gets an available job from the group (queue, experiment, etc.).  An optional
        worker id can be assigned.  After the job is allocated to the function,
        several job characteristics are updated.

        Input:  credentials
                group: str, name of groupname in table
                worker: str, id for worker
        Output: A job record from jobqueue table
    """
    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        tic = time.time()
        cmd = """
                            UPDATE {}
                            SET status = 'running'
                                host = %s,
                                worker = %s,
                                start_time = current_timestamp,
                                update_time = current_timestamp
                            WHERE uuid = (SELECT uuid 
                                          FROM {} 
                                          WHERE status IS NULL  
                                            AND groupname = %s
                                          ORDER BY priority ASC, random()
                                          LIMIT 1 FOR UPDATE)
                            RETURNING *
                            """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name),
                                  sql.Identifier(table_name))
        cursor.execute(cmd, [hostname, worker, group])
        return cursor.fetchone()

    return execute_database_command(credentials, command)


def update_job_status(credentials, uuid):
    """ While a job is being worked on, the worker can periodically let the queue know it is still working on the
        job (instead of crashed or frozen).

        Input:  credentials
                uuid, str: the id of the job in the table

        Output: None
    """

    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        cmd = sql.SQL(
            """
            UPDATE {}
            SET update_time = current_timestamp
            WHERE uuid = %s
            """).format(sql.Identifier(table_name))
        cursor.execute(cmd, [uuid])

    execute_database_command(credentials, command)


def mark_job_as_done(credentials, uuid):
    """ When a job is finished, this function will mark the status as done.

        Input:  credentials
                uuid, str: the id of the job in the table

        Output: None
    """

    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        cmd = sql.SQL("""
                UPDATE {}
                SET status = 'done',
                    update_time = current_timestamp,
                    end_time = current_timestamp 
                WHERE uuid = %s
                """).format(sql.Identifier(table_name))
        cursor.execute(cmd, [uuid])

    execute_database_command(credentials, command)


def clear_queue(credentials, groupname):
    """ Clears all records for a given group.

        Input: credentials
                groupname, str: Name of group (queue)
        Output: None
    """

    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        cmd = """
                   DELETE FROM {} WHERE groupname = %s;    
                   """
        data = [groupname]
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        cursor.execute(cmd, data)

    execute_database_command(credentials, command)


def clear_table(credentials):
    """ Clears all records in the table.

        Input: credentials
        Output: None
    """

    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        cursor.execute(sql.SQL('DELETE FROM {};').format(sql.Identifier(table_name)))

    execute_database_command(credentials, command)


def get_dataframe(credentials):
    """ Returns the entire table as a dataframe.
        Input: credentials
        Output: pandas.DataFrame
    """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials)
    cmd = sql.SQL('SELECT * FROM {}').format(sql.Identifier(table_name))
    df = pd.read_sql(cmd, con=connection)
    return df


def get_messages(credentials, group):
    """ Returns the count of open jobs for a given group.
    Input: credentials, group
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


def get_message_counts(credentials, group):
    """ Returns the count of open, pending, and completed jobs for a given group.
   Input: credentials, group
   Output: (num open, num pending, num completed)
   """
    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        cursor.execute(sql.SQL("""
                        SELECT status, count(*) FROM {}
                        WHERE groupname = %s
                        GROUP BY status;
                       """).format(sql.Identifier(table_name)),
                       [group])
        return cursor.fetchall()

    records = {r[0]: r[1] for r in execute_database_command(credentials, command)}
    return records.get(None, 0), records.get(None, 'running'), records.get(None, 'done')


def reset_incomplete_jobs(credentials, group, interval='0 hours'):
    """ Mark all incomplete jobs in a given group started more than {interval} ago as open.

        Input:  credentials
                group, str: the group name
                interval, str: Postgres interval string specifying time from now, before which jobs will be reset

        Output: None
    """
    table_name, credentials = get_table_name(credentials)

    def command(cursor):
        cmd = """
                UPDATE {}
                SET status = null,
                    start_time = null,
                    host = null,
                    aquire = null
                WHERE groupname = %s
                    and status = 'running'
                    and update_time < current_timestamp - interval %s;
                """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        cursor.execute(cmd, [group, interval])

    execute_database_command(credentials, command)
