from enum import IntEnum


class JobStatus(IntEnum):
    Failed = -1
    Queued = 0
    Running = 1
    Complete = 2
