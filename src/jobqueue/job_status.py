from enum import IntEnum


class JobStatus(IntEnum):
    Queued = 0
    Claimed = 1
    Complete = 2
    Failed = 3
    Disabled = 4
