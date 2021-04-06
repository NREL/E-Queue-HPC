import random
import jobqueue

def generate_job():
    tmp = {
        "sleep": random.random()
    }
    return tmp



if __name__ == "__main__":

    jq = jobqueue.JobQueue("test", 'test_queue')
    jq.clear()

    for i in range(100):
        job = generate_job()
        jq.add_job(job)

    print(jq.messages)


    