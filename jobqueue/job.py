import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from functools import total_ordering
from jobqueue.job_status import JobStatus


def _make_priority():
    return (int(datetime.utcnow().timestamp()) - 1639549880)


@total_ordering
@dataclass
class Job:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    parent: Optional[uuid.UUID] = None
    status: JobStatus = JobStatus.Queued
    priority: Optional[int] = field(default_factory=_make_priority)
    command: Any = None

    error_count: int = 0
    error: Optional[str] = None

    start_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    worker: Optional[int] = None

    def __eq__(self, other):
        return self.id == other.id

    def __lt__(self, other):
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.id < other.id

    def __hash__(self):
        return self.id.__hash__()
