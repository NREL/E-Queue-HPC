import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


def _make_priority():
    return (int(datetime.utcnow().timestamp()) - 1639549880)


@dataclass
class Job:
    id: Optional[int] = None
    priority: Optional[int] = field(default_factory=_make_priority)

    run_count: int = 0
    start_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    worker: Optional[uuid.UUID] = None
    error: Optional[str] = None

    parent: Optional[uuid.UUID] = None
    depth: int = 0
    command: any = None
