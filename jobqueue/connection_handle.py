import sys

from jobqueue.functions import connect, release_pooled_connection


class ConnectionHandle:

    def __init__(self, credentials: {str, any}):
        self._credentials: {str, any} = credentials
        self._connection = None
        self._cursor = None
        self._pooling: bool = False

    def __enter__(self):
        try:
            self._pooling = self._credentials.get('pooling', False)
            self._connection = connect(self._credentials)
            self._cursor = self._connection.cursor()
            return self._cursor
        except Exception as e:
            self.__exit__(*sys.exc_info())
            raise e

    def __exit__(self, type, value, traceback):
        e = None
        try:
            if self._cursor is not None:
                try:
                    self._cursor.close()
                except Exception as e:
                    e = e


                if self._connection is not None:
                    try:
                        if self._pooling:
                            release_pooled_connection(self._credentials, self._connection)
                        else:
                            self._connection.close()
                    except Exception as e:
                        e = e
            except Exception as e:
                pass
        finally:
            self._connection = None
            self._cursor = None
        return False
