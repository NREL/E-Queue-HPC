import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from jobqueue.job_status import JobStatus


def _make_priority():
    return (int(datetime.utcnow().timestamp()) - 1639549880)


@dataclass
class Job:
    id: Optional[uuid.UUID] = field(default_factory=uuid.uuid4)
    priority: Optional[int] = field(default_factory=_make_priority)
    command: any = None

    error_count: int = 0
    error: Optional[str] = None

    status: JobStatus = JobStatus.Queued

    start_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    worker: Optional[int] = None
