from dataclasses import dataclass


@dataclass
class Group:
    task_id: str
    group_name: str
    def __str__(self):
        return f"TabGroups(task_id={self.task_id},group_name={self.group_name})"