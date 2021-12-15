import json
import os
import random
import uuid

import jobqueue
import pytest
from jobqueue.connect import load_credentials
from jobqueue.job import Job
from jobqueue.job_queue import JobQueue

filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
databases = json.loads(open(filename).read())


# @pytest.mark.parametrize("pooling", [True, False])
# @pytest.mark.parametrize("queue", list(range(10)))
def test_functions(pooling=False, queue=0):

    print('begin test *****')

    queue = random.randint(-2**15, 2**15)

    credentials = load_credentials('test')
    credentials['pooling'] = pooling

    queue = JobQueue(credentials, queue, check_table=True)
    queue.clear()

    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 0

    queue.push(Job(command={'id': 1}))
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 1

    queue.push(Job(command={'id': 2}))
    queue.push_multiple([Job(command={'id': 3})])
    queue.push_multiple(())
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 3

    queue.push(Job(command={'id': 4}))
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 4

    queue.push_multiple([Job(command={'id': i}) for i in range(5, 8)])
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 7

    j1 = queue.pop()

    worker_id_1 = uuid.uuid4()
    j2 = queue.pop(worker_id=worker_id_1)

    worker_id_2 = uuid.uuid4()
    j3 = queue.pop_multiple(worker_id_2, num_jobs=3)
    print(j1)
    print(j2)
    print(j3)

    queue.mark_job_complete(j1.id)
    df = queue.get_jobs_as_dataframe()
    print(df)

    queue.mark_job_failed(j2.id, 'test failure')
    df = queue.get_jobs_as_dataframe()
    print(df)

    queue.update_job(j3[0].id)
    df = queue.get_jobs_as_dataframe()
    print(df)

    queue_length = queue.get_queue_length()
    print(queue_length)


test_functions()
