import json
import os

import pytest

import jobqueue
from jobqueue.connect import load_credentials
from jobqueue.job import Job
from jobqueue.job_queue import JobQueue

filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
databases = json.loads(open(filename).read())


@pytest.mark.parametrize("pooling", [True, False])
@pytest.mark.parametrize("queue", [-768, -1, 0, 1, 1024, 8191])
def test_functions(pooling, queue):

    print('begin test *****')

    credentials = load_credentials('test')
    credentials['pooling'] = pooling

    queue = JobQueue(credentials, queue, check_table=True)
    queue.clear()

    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 0

    queue.push([Job(command={'id': 1})])
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 1

    queue.push([Job(command={'id': 2})])
    queue.push([Job(command={'id': 3})])
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 3

    queue.push([Job(command={'id': 4})])
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 4

    queue.push([Job(command={'id': i}) for i in range(5, 8)])
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 7

    j1 = queue.pop(num_jobs=1)
    j2 = queue.pop(num_jobs=1)
    j3 = queue.pop(num_jobs=3)
    print(j1)
    print(j2)
    print(j3)

    # res1 = jobqueue.functions.pop_jobs(credentials, table_name, "test", worker=1)
    # res2 = jobqueue.functions.pop_jobs(credentials, table_name, "test", worker="abc")

    # jobqueue.functions.add_job(credentials, table_name, "test", {'id': 1})
    # jobqueue.functions.add_job(credentials, table_name, "test", {'id': 2})
    # jobqueue.functions.add_job(credentials, table_name, "test", {'id': 3})

    # credentials = databases["test"].copy()
    # # credentials['table_name'] = "test_jobqueue"

    # if pooling:
    #     credentials['pooling'] = True
    # else:
    #     del credentials['pooling']

    # # Create table
    # table_name = credentials['table_name']
    # del credentials['table_name']
    # jobqueue.functions._create_tables(credentials, table_name, drop_table=True)
    # jobqueue.functions.clear_all_queues(credentials, table_name)

    # df = jobqueue.functions.get_jobs_as_dataframe(credentials, table_name)
    # assert len(df) == 0

    # # Add two jobs to group "test"
    # jobqueue.functions.add_job(credentials, table_name, "test", {'id': 1})
    # jobqueue.functions.add_job(credentials, table_name, "test", {'id': 2})
    # jobqueue.functions.add_job(credentials, table_name, "test", {'id': 3})

    # df = jobqueue.functions.get_jobs_as_dataframe(credentials, table_name)
    # assert len(df) == 3

    # # Add two jobs to group "test2"
    # jobqueue.functions.add_job(credentials, table_name, "test2", {'id': 1}, priority=2)
    # jobqueue.functions.add_job(credentials, table_name, "test2", {'id': 2}, priority=1)  # lowest
    # jobqueue.functions.add_job(credentials, table_name, "test2", {'id': 3}, priority=3)
    # df = jobqueue.functions.get_jobs_as_dataframe(credentials, table_name)
    # assert len(df) == 6

    # # fetch jobs from "test2" with workers
    # res0 = jobqueue.functions.pop_jobs(credentials, table_name, "test2")
    # assert res0[2]['id'] == 2

    # # fetch jobs from "test" with workers
    # res0 = jobqueue.functions.pop_jobs(credentials, table_name, "test")
    # res1 = jobqueue.functions.pop_jobs(credentials, table_name, "test", worker=1)
    # res2 = jobqueue.functions.pop_jobs(credentials, table_name, "test", worker="abc")

    # print(res0)
    # print(res1)
    # print(res2)

    # # mark job as complete
    # jobqueue.functions.mark_job_complete(credentials, table_name, res0[0])
    # jobqueue.functions.mark_job_complete(credentials, table_name, res1[0])
    # jobqueue.functions.mark_job_complete(credentials, table_name, res2[0])

    # # clear the "test2" queue
    # jobqueue.functions.clear_queue(credentials, table_name, "test2")
    # df = jobqueue.functions.get_jobs_as_dataframe(credentials, table_name)
    # assert len(df) == 3
