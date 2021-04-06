import subprocess
import time

import jobqueue

if __name__ == "__main__":

    jq = jobqueue.JobQueue("test", 'test_queue')
    print(f"messages = {jq.messages}")

    cmd = "python run_jobs.py"
    procs = []
    for i in range(4):
        procs.append(subprocess.Popen(cmd, shell=True))

    while jq.messages > 0:
        time.sleep(5)
        print(f"messages = {jq.messages}")

    # wait for all
    [ x.wait() for x in procs ]

    print(f"messages = {jq.messages}")