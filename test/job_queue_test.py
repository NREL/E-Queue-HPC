import json
import os
import random
from typing import Dict, List, Tuple
import uuid
import numpy

import heapq
import pandas

import jobqueue
import pytest
from jobqueue.connect import load_credentials
from jobqueue.job import Job
from jobqueue.job_queue import JobQueue
from jobqueue.job_status import JobStatus

filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
databases = json.loads(open(filename).read())


# @pytest.mark.parametrize("pooling", [True, False])
# @pytest.mark.parametrize("queue", list(range(10)))
def test_functions(pooling=False, queue=0):
    print('begin test *****')

    credentials = load_credentials('test')
    credentials['pooling'] = pooling

    queue_id = 0
    queue = JobQueue(credentials, queue_id, check_table=True)
    queue.clear()

    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 0

    queue.push(Job(command={'key': 1}))
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 1

    queue.push(Job(command={'key': 2}))
    queue.push([Job(command={'key': 3})])
    queue.push(())
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 3

    queue.push(Job(command={'key': 4}))
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 4

    queue.push([Job(command={'key': i}) for i in range(5, 8)])
    df = queue.get_jobs_as_dataframe()
    print(df)
    assert len(df) == 7

    j1 = queue.pop()

    worker_id_1 = uuid.uuid4()
    j2 = queue.pop(worker_id=worker_id_1)

    worker_id_2 = None
    j3 = queue.pop(worker_id=worker_id_2, n=3)
    print(j1)
    print(j2)
    print(j3)

    queue.complete(j1)
    df = queue.get_jobs_as_dataframe()
    print(df)

    queue.fail(j2, 'test failure')
    df = queue.get_jobs_as_dataframe()
    print(df)

    queue.update(j3[0])
    df = queue.get_jobs_as_dataframe()
    print(df)

    queue_length = queue.get_queue_length()
    print(queue_length)

    queue.clear()


def test_single_worker(pooling=False, queue=0, total_actions=100):
    job_map: Dict[uuid.UUID, Job] = {}
    # jobs_claimed: List[Tuple[int, Job]] = []
    claimed_heap: List[Tuple[int, Job]] = []
    jobs_queued = 0
    # jobs_claimed = 0
    jobs_completed = 0
    jobs_failed = 0

    credentials = load_credentials('test')
    credentials['pooling'] = pooling

    queue_id = 0
    queue = JobQueue(credentials, queue_id, check_table=True)
    queue.clear()

    worker_id = uuid.uuid4()

    # def pop_job():
    #     while len(claimed_heap) > 0:
    #         j = heapq.heappop(claimed_heap)[1]
    #         if j.status == JobStatus.Claimed:
    #             return j
    #     return None

    max_trend_length = 10000
    max_push = 16
    max_pop = 16
    trend_switch = total_actions
    fractions = None
    fail_fraction = 0.0
    single_pop_fraction = 0.0

    while total_actions > 0:
        # push, pop, update, complete/fail
        if total_actions <= trend_switch:
            trend_switch = total_actions - random.randint(1, max_trend_length)

            fractions = numpy.random.random_sample((4,))
            fractions = numpy.cumsum(fractions / numpy.sum(fractions))
            fail_fraction = random.random() ** 4
            single_pop_fraction = random.random()

        action = random.random()
        total_actions -= 1

        if action < fractions[0] and len(claimed_heap) > 0:
            # remove a claimed job by failing or completing it
            j = None
            while len(claimed_heap) > 0:
                j = heapq.heappop(claimed_heap)[1]
                if j.status == JobStatus.Claimed:
                    break

            if j is not None:
                if j.status == JobStatus.Claimed:
                    if random.random() < fail_fraction:
                        # fail
                        queue.fail(j, "Test error.")
                        j.status = JobStatus.Failed
                        jobs_failed += 1
                    else:
                        # complete
                        j.status = JobStatus.Complete
                        jobs_completed += 1
            continue

        if action < fractions[1] and len(claimed_heap) > 0:
            # update a claimed job
            j = random.choice(claimed_heap)[1]
            if j.status == JobStatus.Claimed:
                queue.update(j)
                # TODO: also update original copy and check later?
            continue

        if action < fractions[2] and jobs_queued > 0:
            # pop
            num_to_pop = random.randint(0, max_pop)
            total_actions -= max(num_to_pop - 1, 0)
            jobs = None
            if random.random() < single_pop_fraction:
                jobs = []
                for _ in range(num_to_pop):
                    j = queue.pop(worker_id=worker_id)
                    if j is not None:
                        jobs.append(j)
            else:
                jobs = queue.pop(num_to_pop, worker_id=worker_id)

            for j in jobs:
                assert j.id in job_map
                o = job_map[j.id]

                assert j.id == o.id
                assert j.status == JobStatus.Claimed
                assert o.status == JobStatus.Queued

                assert j.priority == o.priority
                assert json.dumps(o.command, sort_keys=True) == \
                    json.dumps(j.command, sort_keys=True)

                o.status = JobStatus.Claimed
                heapq.heappush(claimed_heap, (random.randint(0, 1000), o))

            # jobs_claimed += len(jobs)
            jobs_queued -= len(jobs)
            continue

        # push
        num_to_push = random.randint(0, max_push)
        total_actions -= max(num_to_push - 1, 0)
        jobs = [Job(priority=random.randint(-int(2 ** 15), int(2**15)-1))
                for _ in range(num_to_push)]
        

        if random.random() < single_pop_fraction:
            for j in jobs:
                queue.push(j)
        else:
            queue.push(jobs)

        for j in jobs:
            job_map[j.id] = j

        jobs_queued += len(jobs)
        continue

    queue.clear()


test_single_worker()
