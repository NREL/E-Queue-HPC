import copy
import os
import json
from . import functions
import uuid
from typing import Optional

class Message:

    def __init__(self, credentials: {str: any}, table_name: str, result: list) -> None:
        self._credentials: {str: any} = credentials
        self._table_name: str = table_name
        self._uuid: uuid.UUID = uuid.UUID(result[0])
        self._config: dict = result[2]
        self._priority: str = result[8]

    @property
    def config(self) -> {str: any}:
        return self._config

    @property
    def uuid(self) -> uuid.UUID:
        return self._uuid

    @property
    def priority(self) -> str:
        return self._priority

    def mark_complete(self) -> None:
        functions.mark_job_as_done(self._credentials, self._table_name, self._uuid)


class JobQueue:

    def __init__(self, database: str, queue: str, _table_name=None) -> None:
        """ Interface to the jobsque database table
            database: str, name of the key in your .jobsqueue.json file.
            queue: str, name of the queue you'd like to create or use.
            _table_name is use for testing only.  To change your table name use the 
            .jobsqueue.json file.
        """
        self._database: str = database
        self._queue: str = queue
        try:
            filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
            _data = json.loads(open(filename).read())
            self._credentials = _data[self._database]
            # used for testing.  The table name should be in the credentials file.
            if _table_name is None:
                _table_name = self._credentials['table_name']
                del self._credentials['table_name']

            self._table_name: str = _table_name
        except KeyError as e:
            raise Exception("No credentials for {} found in {}".format(database, filename))

        # ensure table exists
        functions.create_table(self._credentials, self._table_name)

    @property
    def messages(self) -> int:
        res = functions.get_messages(self._credentials, self._table_name, self._queue)
        return res[0]

    @property
    def message_counts(self) -> (int, int, int):
        return functions.get_message_counts(self._credentials, self._table_name, self._queue)

    def clear(self) -> None:
        functions.clear_queue(self._credentials, self._table_name, self._queue)

    def get_message(self, worker=None) -> Optional[Message]:
        res = functions.fetch_job(self._credentials, self._table_name, self._queue, worker=worker)
        if res is None:
            return None
        return Message(self._credentials, self._table_name, res)

    def add_job(self, job, priority=None) -> None:
        functions.add_job(self._credentials, self._table_name, self._queue, job, priority=priority)

    def reset_incomplete_jobs(self, interval='4 hours') -> None:
        functions.reset_incomplete_jobs(self._credentials, self._table_name, self._queue, interval=interval)

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
