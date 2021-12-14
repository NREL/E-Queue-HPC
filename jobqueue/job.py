import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from jobqueue.job_queue import JobQueue

from .job_status import JobStatus


@dataclass
class Job:
    id: Optional[uuid.UUID] = None  # = field(default_factory=uuid.uuid4)
    priority: Optional[int] = None

    run_count: int = 0
    start_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    worker: Optional[uuid.UUID] = None
    error: Optional[str] = None

    parent: Optional[uuid.UUID] = None
    depth: int = 0
    command: any = None
