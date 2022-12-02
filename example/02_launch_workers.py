# import subprocess
# import time

# import jobqueue

# if __name__ == "__main__":

#     job_queue = jobqueue.JobQueue("test", 'test_queue')
#     print(f"messages = {job_queue.messages}")

#     cmd = "python run_jobs.py"
#     procs = []
#     for i in range(4):
#         procs.append(subprocess.Popen(cmd, shell=True))

#     while job_queue.messages > 0:
#         time.sleep(5)
#         print(f"messages = {job_queue.messages}")

#     # wait for all
#     [ x.wait() for x in procs ]

#     print(f"messages = {job_queue.messages}")