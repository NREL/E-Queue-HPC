from setuptools import setup


setup(name='jobqueue',
      version='0.0.1',
      install_requires=['pytest', 'pandas', 'psycopg2-binary'],  # list dependencies
      entry_points={ "console_scripts": [
                        "jq=jobqueue.main:main",
            ]},
      )