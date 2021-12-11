import sys

from psycopg2._psycopg import cursor

from jobqueue.connect import connect, release_pooled_connection
from typing import Dictionary


class CursorManager:

    def __init__(self, credentials: Dictionary[str, any], autocommit: bool = True):
        self._credentials: Dictionary[str, any] = credentials
        self._autocommit: bool = autocommit
        self._connection = None
        self._pooling: bool = False
        self._cursor = None

    def __enter__(self) -> cursor:
        try:
            self._pooling = self._credentials.get('pooling', False)
            connection = connect(self._credentials)
            self._connection = connection
            connection.autocommit = self._autocommit
            self._cursor = connection.cursor()
            return self._cursor
        except Exception as e:
            if not self.__exit__(*sys.exc_info()):
                raise e

    def __exit__(self, exception_type, exception_value, traceback):
        connection = self._connection
        self._connection = None

        cursor = self._cursor
        self._cursor = None

        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                if exception_value is None:
                    exception_value = sys.exc_info()

        if connection is not None:
            try:
                if self._pooling:
                    release_pooled_connection(self._credentials, connection)
                else:
                    connection.close()
            except Exception as e:
                if exception_value is None:
                    exception_value = sys.exc_info()

        if exception_value is not None and exception_type is None:
            raise exception_value

        return False
