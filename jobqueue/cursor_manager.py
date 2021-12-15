import sys
from typing import Dict

from psycopg2._psycopg import cursor

from jobqueue.connect import connect, release_pooled_connection


class CursorManager:

    def __init__(self, credentials: Dict[str, any], autocommit: bool = True):
        self._credentials: Dict[str, any] = credentials
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

        exception = exception_value

        def do_and_capture(func):
            nonlocal exception
            try:
                func()
            except Exception as e:
                if exception is None:
                    exception = e

        do_and_capture(lambda: cursor.close())

        if self._pooling:
            do_and_capture(lambda: release_pooled_connection(
                self._credentials, connection))
        else:
            do_and_capture(lambda: connection.close())

        if exception is not None:
            raise exception

        return False
