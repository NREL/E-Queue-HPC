"""
Author: Monte Lunacek, Charles Tripp


    This file contains basic functions that execute SQL commands on a
    database given a set credentials, for example:

    credentials = {
        "host": "yuma.hpc.nrel.gov",
        "user": "dmpappsops",
        "database": "dmpapps",
        "password": "*****************",
        "table_name": "jobqueue"
    }

    This can be passed to Any function that has already created the jobsqueue
    table.
"""

import json
import os
import random
import time
from typing import Any, Dict

import psycopg
from psycopg_pool import ConnectionPool


"""
Stores connection pools, indexed by credential object id.
"""
_connection_pools: Dict[int, Any] = {}


def load_credentials(database: str) -> Dict[str, Any]:
    filename = os.path.join(os.environ["HOME"], ".jobqueue.json")
    try:
        data = json.loads(open(filename).read())
        return data[database]
    except KeyError:
        raise Exception("No credentials for {} found in {}".format(database, filename))


def close_pools() -> None:
    for pool in _connection_pools.values():
        pool.closeall()


def _get_pool(credentials: Dict[str, Any]) -> ConnectionPool:
    credential_id = id(credentials)
    if credential_id in _connection_pools:
        pool = _connection_pools[credential_id]
    else:
        inner_credentials = _extract_inner_credentials(credentials)
        pool = ConnectionPool(minconn=0, maxconn=2, **inner_credentials)
        _connection_pools[credential_id] = pool
    return pool


def acquire_pooled_connection(credentials: Dict[str, Any]) -> Any:
    return _get_pool(credentials).getconn()


def release_pooled_connection(credentials: Dict[str, Any], connection: Any) -> None:
    _get_pool(credentials).putconn(connection)


def connect(credentials: Dict[str, Any], autocommit: bool = True) -> Any:
    pooling = credentials.get("pooling", False)
    connection = None
    if pooling:
        connection = acquire_pooled_connection(credentials)
    else:
        initial_wait_max = credentials.get("initial_wait_max", 120)
        min_wait = credentials.get("min_wait", 0.5)
        max_wait = credentials.get("max_wait", 2 * 60 * 60)
        max_attempts = credentials.get("max_attempts", 10000)
        attempts = 0
        while attempts < max_attempts:
            wait_time = 0.0
            try:
                inner_credentials = _extract_inner_credentials(credentials)
                connection = psycopg.connect(
                    " ".join(
                        f"{key}={value}" for key, value in inner_credentials.items()
                    ),
                    autocommit=autocommit,
                )
            except psycopg.OperationalError as e:
                print(f"OperationalError while connecting to database: {e}", flush=True)

                sleep_time = random.uniform(min_wait, max(initial_wait_max, wait_time))
                wait_time += sleep_time
                attempts += 1

                if attempts >= max_attempts or wait_time >= max_wait:
                    raise e
                time.sleep(sleep_time)

    connection.autocommit = autocommit
    return connection


def _extract_inner_credentials(credentials: Dict[str, Any]) -> Dict[str, Any]:
    return {k: credentials[k] for k in ("host", "dbname", "user", "password")}
