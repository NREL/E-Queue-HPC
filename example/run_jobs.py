import time
import jobqueue

def run_job(message):
    if message is not None:
        time.sleep(message.config['sleep'])
        message.mark_complete()

if __name__ == "__main__":

    job_queue = jobqueue.JobQueue("test", 'test_queue')

    while job_queue.messages > 0:
        
        message = job_queue.get_message()
        run_job(message)
        

