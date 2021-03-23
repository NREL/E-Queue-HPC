
from jobqueue.functions import create_table
import os
import json

import jobqueue

filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
databases = json.loads(open(filename).read())


def test_functions():
    # Create table
    credentials = databases["dmp"]
    credentials['table_name'] = "test_jobqueue"
    jobqueue.functions.create_table(credentials)
    jobqueue.functions.clear_table(credentials)

    df = jobqueue.functions.get_dataframe(credentials)
    assert len(df) == 0

    # Add two jobs to group "test"
    jobqueue.functions.add_job(credentials, "test", {'id': 1})
    jobqueue.functions.add_job(credentials, "test", {'id': 2})
    jobqueue.functions.add_job(credentials, "test", {'id': 3})
    
    df = jobqueue.functions.get_dataframe(credentials)
    assert len(df) == 3

    # Add two jobs to group "test2"
    jobqueue.functions.add_job(credentials, "test2", {'id': 1})
    jobqueue.functions.add_job(credentials, "test2", {'id': 2})
    df = jobqueue.functions.get_dataframe(credentials)
    assert len(df) == 5

    # fetch jobs from "test" with workers
    res0 = jobqueue.functions.fetch_job(credentials, "test")    
    res1 = jobqueue.functions.fetch_job(credentials, "test", worker=1)    
    res2 = jobqueue.functions.fetch_job(credentials, "test", worker="abc")  

    print(res0)
    print(res1)
    print(res2)

    # mark job as complete
    jobqueue.functions.mark_job_as_done(credentials, res0[0])
    jobqueue.functions.mark_job_as_done(credentials, res1[0])
    jobqueue.functions.mark_job_as_done(credentials, res2[0])

    # clear the "test2" queue
    jobqueue.functions.clear_queue(credentials, "test2")
    df = jobqueue.functions.get_dataframe(credentials)
    assert len(df) == 3






