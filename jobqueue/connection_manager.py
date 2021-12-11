import sys

from psycopg2._psycopg import cursor

from jobqueue.connections import connect, release_pooled_connection
from typing import Dictionary


class ConnectionManager:

    def __init__(self, credentials: Dictionary[str, any], autocommit: bool = True):
        self._credentials: Dictionary[str, any] = credentials
        self._autocommit: bool = autocommit
        self._connection = None
        self._pooling: bool = False

    def __enter__(self) -> cursor:
        try:
            self._pooling = self._credentials.get('pooling', False)
            connection = connect(self._credentials)
            self._connection = connection
            connection.autocommit = self._autocommit
            return connection
        except Exception as e:
            if not self.__exit__(*sys.exc_info()):
                raise e

    def __exit__(self, exception_type, exception_value, traceback):
        connection = self._connection
        self._connection = None

        if connection is not None:
            try:
                if self._pooling:
                    release_pooled_connection(self._credentials, connection)
                else:
                    connection.close()
            except Exception as e:
                if exception_value is None:
                    raise e

        return False
