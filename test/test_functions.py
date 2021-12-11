import json
import os

import pytest

import jobqueue

filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
databases = json.loads(open(filename).read())


@pytest.mark.parametrize("pooling", [True, False])
def test_functions(pooling):
    credentials = databases["test"].copy()
    # credentials['table_name'] = "test_jobqueue"

    if pooling:
        credentials['pooling'] = True
    else:
        del credentials['pooling']

    # Create table
    table_name = credentials['table_name']
    del credentials['table_name']
    jobqueue.functions.create_tables(credentials, table_name, drop_table=True)
    jobqueue.functions.clear_all_queues(credentials, table_name)

    df = jobqueue.functions.get_jobs_as_dataframe(credentials, table_name)
    assert len(df) == 0

    # Add two jobs to group "test"
    jobqueue.functions.add_job(credentials, table_name, "test", {'id': 1})
    jobqueue.functions.add_job(credentials, table_name, "test", {'id': 2})
    jobqueue.functions.add_job(credentials, table_name, "test", {'id': 3})

    df = jobqueue.functions.get_jobs_as_dataframe(credentials, table_name)
    assert len(df) == 3

    # Add two jobs to group "test2"
    jobqueue.functions.add_job(credentials, table_name, "test2", {'id': 1}, priority=2)
    jobqueue.functions.add_job(credentials, table_name, "test2", {'id': 2}, priority=1)  # lowest
    jobqueue.functions.add_job(credentials, table_name, "test2", {'id': 3}, priority=3)
    df = jobqueue.functions.get_jobs_as_dataframe(credentials, table_name)
    assert len(df) == 6

    # fetch jobs from "test2" with workers
    res0 = jobqueue.functions.pop_jobs(credentials, table_name, "test2")
    assert res0[2]['id'] == 2

    # fetch jobs from "test" with workers
    res0 = jobqueue.functions.pop_jobs(credentials, table_name, "test")
    res1 = jobqueue.functions.pop_jobs(credentials, table_name, "test", worker=1)
    res2 = jobqueue.functions.pop_jobs(credentials, table_name, "test", worker="abc")

    print(res0)
    print(res1)
    print(res2)

    # mark job as complete
    jobqueue.functions.mark_job_complete(credentials, table_name, res0[0])
    jobqueue.functions.mark_job_complete(credentials, table_name, res1[0])
    jobqueue.functions.mark_job_complete(credentials, table_name, res2[0])

    # clear the "test2" queue
    jobqueue.functions.clear_queue(credentials, table_name, "test2")
    df = jobqueue.functions.get_jobs_as_dataframe(credentials, table_name)
    assert len(df) == 3
