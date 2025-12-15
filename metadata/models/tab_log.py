from dataclasses import dataclass
from datetime import datetime

from metadata.models.tab_tasks import Task
from processor.domain import TaskState


@dataclass
class TaskLog:
    task: str
    run_id: str
    state: TaskState
    description: str
    error_message: str
    update_ts: datetime
    task_group: str