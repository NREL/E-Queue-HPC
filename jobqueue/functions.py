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
    connection = psycopg2.connect(**credentials) 
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    cursor.close()
    connection.close()
    return f"You are connected to - {record}"
    
def create_table(credentials):
    """ Creates the table if it does not exists
        Input: credentials
        Output: None

    """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials)  
    cmd = """
    CREATE TABLE IF NOT EXISTS {}(
        UUID            VARCHAR NOT NULL PRIMARY KEY,
        USERNAME        VARCHAR NOT NULL,
        CONFIG          JSON    NOT NULL,
        GROUPNAME           VARCHAR,
        HOST            VARCHAR,
        STATUS          VARCHAR,
        WORKER          VARCHAR,
        creation_time   timestamp,
        priority        VARCHAR,
        start_time      timestamp,
        end_time        timestamp,
        depth           integer,
        wall_time       float,
        aquire          float
    );
    """
    try:
        cursor = connection.cursor()
        args = (table_name, table_name)
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        cursor.execute(cmd)
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
    finally:
        if connection:
            cursor.close()    
    connection.close()

def recreate_table(credentials):
    """ Deletes and replaces or creates the current jobqueue table.
        Input: credentials
        Output: None
    """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials)  
    cmd = """
    DROP TABLE IF EXISTS {};
    CREATE TABLE {}(
        UUID            VARCHAR NOT NULL PRIMARY KEY,
        USERNAME        VARCHAR NOT NULL,
        CONFIG          JSON    NOT NULL,
        GROUPNAME           VARCHAR,
        HOST            VARCHAR,
        STATUS          VARCHAR,
        WORKER          VARCHAR,
        creation_time   timestamp,
        priority        VARCHAR,
        start_time      timestamp,
        end_time        timestamp,
        depth           integer,
        wall_time       float,
        aquire          float
    );
    """
    try:
        cursor = connection.cursor()
        args = (table_name, table_name)
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name), 
                                  sql.Identifier(table_name))
        cursor.execute(cmd)
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
    finally:
        if connection:
            cursor.close()    
    connection.close()

def add_job(credentials, group, job, priority=None):
    """ Adds a job (dictionary) to the database jobqueue table.
        Input:  credentials
                group: str, name of "queue" in the database
                job: dict, must be able to call json.dumps
        Output: None
    """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials) 
    try:
        
        job_id = job.get('uuid', str(uuid.uuid4()))
        user = os.environ.get('USER')
        now = datetime.datetime.now()
        if priority is None:
            priority = str(now)
     
        cmd = """
            INSERT INTO {}(uuid, username, config, groupname, 
                                 host, status, worker, creation_time, priority) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        
        args = (job_id, user, json.dumps(job), group, None, None, None, now, priority)
        
        cursor = connection.cursor()
        cursor.execute(cmd, args)
        connection.commit()
            
        cursor.close()
        connection.close()
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

        
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
    tic = time.time()
    cmd = """
            UPDATE {}
            SET status = 'running'
            WHERE uuid = (SELECT uuid 
                          FROM {} 
                          WHERE status IS NULL 
                                and groupname = %s
                          ORDER BY priority
                          LIMIT 1 FOR UPDATE)
            RETURNING *       
            """
    connection = psycopg2.connect(**credentials)     
    try:
        cursor = connection.cursor()
        data = [group]
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name),
                                  sql.Identifier(table_name))
        cursor.execute(cmd, data)
        record = cursor.fetchone()
        connection.commit()
        cursor.close()
        
        # update record
        aquire = time.time()-tic
        hostname = socket.gethostname()
        start_time = datetime.datetime.now()
        uuid = record[0]
        cmd = """
            UPDATE {}
            SET aquire = %s,
                host = %s,
                worker = %s,
                start_time = %s
            WHERE uuid = %s     
            """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        data = [aquire, hostname, worker, start_time, uuid]
        cursor = connection.cursor()
        cursor.execute(cmd, data)
        connection.commit()
        cursor.close()
        
        return record
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
        cursor.close()
        return None
    finally:
        if connection:
            cursor.close()
            connection.close() 

def mark_job_as_done(credentials, uuid):
    """ When a job is finished, this function will mark the status as done.

        Input:  credentials
                uuid, str: the id of the job in the table

        Output: None
    """

    cmd = """
        UPDATE {}
        SET status = 'done',
            end_time = %s
        WHERE uuid = %s
        """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials) 
    now = datetime.datetime.now()
    
    try:
        cursor = connection.cursor()
        data = [now, uuid]
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        cursor.execute(cmd, data)
        connection.commit()
        cursor.close()
    
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
        cursor.close()
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()  
            
def clear_queue(credentials, groupname):
    """ Clears all records for a given group.

        Input: credentials
                groupname, str: Name of group (queue)
        Output: None
    """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials) 
    try:
        cmd = """
            DELETE FROM {} WHERE groupname = %s;    
            """
        data = [groupname]
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        cursor = connection.cursor()
        cursor.execute(cmd, data)
        connection.commit()
        
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
        cursor.close()
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()   

def clear_table(credentials):
    """ Clears all records in the table.

        Input: credentials
        Output: None
    """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials) 
    try:
        cmd = """
            DELETE FROM {};    
            """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        cursor = connection.cursor()
        cursor.execute(cmd)
        connection.commit()
        
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
        cursor.close()
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()   


def get_dataframe(credentials):
    """ Returns the entire table as a dataframe.
        Input: credentials
        Output: pandas.DataFrame
    """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials) 
    cmd = "select * from {}"
    cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
    df = pd.read_sql(cmd, con=connection)
    return df

def get_messages(credentials, group):
    """ Returns the entire table as a dataframe.
    Input: credentials
    Output: int, number of open jobs
    """
    table_name, credentials = get_table_name(credentials)
    connection = psycopg2.connect(**credentials) 
    try:
        cmd = """
            select count(*) from {} 
            where groupname = %s and
                  status is null;    
            """
        cmd = sql.SQL(cmd).format(sql.Identifier(table_name))
        data = [group]
        cursor = connection.cursor()
        cursor.execute(cmd, data)
        record = cursor.fetchone()
        return record
        
    except (Exception, psycopg2.Error) as error:
        print("Error", error)
        cursor.close()
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()   