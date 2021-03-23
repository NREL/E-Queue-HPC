# jobqueue-pg

## Installation

Install this module using `pip`:

    pip install --editable=git+https://github.nrel.gov/mlunacek/jobqueue@master#egg=jobqueue

We recommend including this as a pip dependency in your `environment.yml` conda file.  For example:

    "--editable=git+https://github.nrel.gov/mlunacek/jobqueue@master#egg=jobqueue"


## Using the `jobqueue`

### Database access

At the moment, you must have `read/write/create` capabilities on your database.  In order for `jobqueue` to know which databases you have access to you will need to describe these in a *hidden* file located in your home directory called ".jobsqueue.json". The structure should look like the following:

File location: `os.path.join(os.environ['HOME'], ".jobqueue.json")`

        {
            "project1": {
                "host": "yuma.hpc.nrel.gov",
                "user": "project1ops",
                "database": "project1",
                "password": "*****************",
                "table_name": "jobqueue"
            },
            "project2": {
                "host": "yuma.hpc.nrel.gov",
                "user": "project2ops",
                "database": "project2",
                "password": "*****************",
                "table_name": "jobqueue"
            },

        }

### JobQueue module

The best interface is using the `JobQueue` module.  Please see the [test case](test/test_interface.py) for details on using this interface.

### The `jq` command line tool



