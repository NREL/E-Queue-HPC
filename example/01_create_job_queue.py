import random
import jobqueue

def generate_job():
    tmp = {
        "sleep": random.random()
    }
    return tmp



if __name__ == "__main__":

    job_queue = jobqueue.JobQueue("test", 'test_queue')
    job_queue.clear()

    for i in range(100):
        job = generate_job()
        job_queue.add_job(job)

    print(job_queue.messages)


    