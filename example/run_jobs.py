from jobqueue.job_queue import Job
import time
import jobqueue

def run_job(message):
    if message is not None:
        time.sleep(message.config['sleep'])
        message.mark_complete()

if __name__ == "__main__":

    jq = jobqueue.JobQueue("test", 'test_queue')

    while jq.messages > 0:
        
        message = jq.get_message()
        run_job(message)
        

