from jobqueue.job_queue import JobQueue
import os
import sys
import argparse
import json
import pandas as pd

import jobqueue

def get_args():

    job_queue_name = None
    if os.path.exists(".default_queue_name"):
        job_queue_name = open(".default_queue_name").read()
        
    parser = argparse.ArgumentParser()
 
    command = parser.add_subparsers(dest="command")
    list_parser = command.add_parser('list')
    list_parser = list_parser.add_argument("database", help="Name of database")
    
    args = parser.parse_args(sys.argv[1:])
    # save the queue name for next time
    # if args.queue_name is not None:
    #     with open(".jobqueue_name", "w") as outfile:
    #         outfile.write(args.queue_name)

    return args

def main():
    args = get_args()
    print(args.command)
    filename = os.path.join(os.environ['HOME'], ".jobqueue.json")
    database = json.loads(open(filename).read())

    df = jobqueue.functions.get_jobs_as_dataframe(database[args.database])
    tmp = df.groupby(['username', 'groupname', 'status'])['config'].count().reset_index()
    print("")
    print(tmp)
    print("")

    tmp = df.query("status=='done'").copy()

    tmp['time'] = (pd.to_datetime(tmp['end_time']) - pd.to_datetime(tmp['start_time']))
    tmp['time'] = tmp['time'].map(lambda x: x.days + x.seconds)
    summary = tmp.groupby(['username', 'groupname'])['time'].agg(['mean', 'std'])

    print("")
    print(summary)
    print("")