from setuptools import setup, find_packages


setup(
    name="jobqueue",
    version="0.0.1",
    packages=find_packages(include=["jobqueue"]),
    install_requires=[
        "pytest",
        "pandas",
        "psycopg[binary]>=3",
        "psycopg_pool>=3",
    ],  # list dependencies
    entry_points={
        "console_scripts": [
            "jq=jobqueue.main:main",
        ]
    },
)
