# E-Queue HPC

A Task Queue for Coordinating Varied Tasks Across Multiple HPC Resources and HPC Jobs.

The main interface is the `JobQueue` module that connects to a Postgres database and allows users to send JSON objects as task descriptions to the database, which then will return these to individual resources when they request a task.  The software needs to know how to unpack the JSON and execute the correct job from the description.  

If the database is configured to be available from multiple clusters, then the compute nodes at these different clusters can all request tasks from the `JobQueue` and participate in the overall workflow.

## Installation

We build our environment using [conda](https://www.anaconda.com/).  Execute the following steps to install the development environment.

        conda env create
        source activate jobqueue
        python setup.py develop

If you'd like to include this module in an existing project, we recommend including it as a pip dependency in your `environment.yml` conda file.  For example:

    "--editable=git+https://github.com/NREL/E-Queue-HPC.git@master#egg=jobqueue"


## Using the `jobqueue` module

### Database access

At the moment, you must have `read/write/create` capabilities on your database.  In order for `jobqueue` to know which databases you have access to you will need to describe these in a *hidden* file located in your home directory called ".jobqueue.json". The structure should look like the following:

File location: `os.path.join(os.environ['HOME'], ".jobqueue.json")`

        {
            "project1": {
                "host": "HOSTNAME",
                "user": "project1ops",
                "database": "project1",
                "password": "*****************",
                "table_name": "jobqueue"
            },
            "project2": {
                "host": "HOSTNAME",
                "user": "project2ops",
                "database": "project2",
                "password": "*****************",
                "table_name": "jobqueue"
            },

        }

### JobQueue module

The best interface is using the `JobQueue` module.  Please see the [test case](test/job_queue_test.py) for details on using this interface.  To run the test, type:

        pytest -s -v 

