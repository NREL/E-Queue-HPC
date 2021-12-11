from enum import IntEnum


class JobStatus(IntEnum):
    Waiting = 0
    Running = 1
    Failed = 2
    Complete = 3
