import json
import os
import uuid
from typing import Optional

from . import functions


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

    def get_message(self, worker: Optional[uuid.UUID] = None) -> Optional[Message]:
        res = functions.fetch_job(self._credentials, self._table_name, self._queue, worker=worker)
        if res is None:
            return None
        return Message(self._credentials, self._table_name, res)

    def add_job(self, job, priority=None) -> None:
        functions.add_job(self._credentials, self._table_name, self._queue, job, priority=priority)

    def fail_incomplete_jobs(self, interval='4 hours') -> None:
        functions.fail_incomplete_jobs(self._credentials, self._table_name, self._queue, interval=interval)

    def reset_incomplete_jobs(self, interval='4 hours') -> None:
        functions.reset_incomplete_jobs(self._credentials, self._table_name, self._queue, interval=interval)

    def reset_failed_jobs(self) -> None:
        functions.reset_failed_jobs(self._credentials, self._table_name, self._queue)

    def run_worker(self, handler, wait_until_exit=15 * 60, maximum_waiting_time=2 * 60):
        print(f"Job Queue: Starting...")

        worker_id = uuid.uuid4()
        wait_start = None
        while True:

            # Pull job off the queue
            message = self.get_message(worker=worker_id)

            if message is None:

                if wait_start is None:
                    wait_start = time.time()
                    wait_bound = 1
                else:
                    waiting_time = time.time() - wait_start
                    if waiting_time > wait_until_exit:
                        print("Job Queue: No Jobs, max waiting time exceeded. Exiting...")
                        break

                # No jobs, wait and try again.
                print("Job Queue: No jobs found. Waiting...")

                # bounded randomized exponential backoff
                wait_bound = min(maximum_waiting_time, wait_bound * 2)
                time.sleep(random.randint(1, wait_bound))
                continue

            try:
                wait_start = None
                print(f"Job Queue: {message.uuid} running...")

                handler(worker_id, message)  # handle the message
                message.mark_complete()  # Mark the job as complete in the queue.

                print(f"Job Queue: {message.uuid} done.")
            except Exception as e:
                print(f"Job Queue: {message.uuid} Unknown exception in jq_runner: {e}.")
                try:
                    message.mark_failed()
                except Exception as e2:
                    print(
                        f"Job Queue: {message.uuid} exception thrown while marking as failed in jq_runner: {e}, {e2}!")

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
