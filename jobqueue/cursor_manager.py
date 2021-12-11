import sys

from psycopg2._psycopg import cursor

from jobqueue.connections import connect, release_pooled_connection
from jobqueue.connection_manager import ConnectionManager
from typing import Dictionary


class CursorManager:

    def __init__(self, credentials: Dictionary[str, any], autocommit: bool = True):
        self._connection_manager: ConnectionManager = ConnectionManager(
            credentials, autocommit)
        self._cursor = None

    def __enter__(self) -> cursor:
        try:
            connection = self._connection_manager.__enter__()
            self._cursor = connection.cursor()
            return self._cursor
        except Exception as e:
            if not self.__exit__(*sys.exc_info()):
                raise e

    def __exit__(self, exception_type, exception_value, traceback):
        cursor = self._cursor
        self._cursor = None

        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                if exception_value is None:
                    exception_type, exception_value, traceback = sys.exc_info()

        return self._connection_manager.__exit__(exception_type, exception_value, traceback)
