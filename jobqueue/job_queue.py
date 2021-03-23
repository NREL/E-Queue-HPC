import os
import json
from . import functions

class Message:
    
    def __init__(self, credentials, result):
        self._credentials = credentials 
        self._uuid = result[0]
        self._config = result[2]
        
    @property
    def config(self):
        return self._config

    @property
    def uuid(self):
        return self._uuid
    
    def mark_complete(self):
        functions.mark_job_as_done(self._credentials, self._uuid)


class JobQueue:
    
    def __init__(self, database, queue, _table_name=None):
        """ Interface to the jobsque database table
            database: str, name of the key in your .jobsqueue.json file.
            queue: str, name of the queue you'd like to create or use.
            _table_name is use for testing only.  To change your table name use the 
            .jobsqueue.json file.
        """
        self._database = database
        self._queue = queue  
        try:
            filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
            _data = json.loads(open(filename).read())
            self._credentials = _data[self._database]
            # used for testing.  The table name should be in the credentials file.
            if _table_name is not None:
                self._credentials['table_name'] = _table_name
        except KeyError as e:
            raise Exception("No credetials for {} found in {}".format(database, filename))

        # ensure table exists
        functions.create_table(self._credentials)
    
    @property
    def messages(self):
        res = functions.get_messages(self._credentials, self._queue)
        return res[0]

    def clear(self):
        functions.clear_queue(self._credentials, self._queue)

    def get_message(self, worker=None):
        res = functions.fetch_job(self._credentials, self._queue, worker=worker)
        if res is not None:
            return Message(self._credentials, res)
        return None

    def add_job(self, job):
        functions.add_job(self._credentials, self._queue, job)



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
        
