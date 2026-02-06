from dataclasses import dataclass
from typing import Optional, List

from pyspark.sql import DataFrame

from metadata.models.tab_tasks import TaskSemaforo


@dataclass
class TaskContext:
    task: TaskSemaforo
    key: dict
    query_params: dict
    run_id: str
    df: Optional[DataFrame] = None
    #data: Optional[List] = None
    #registro_repo: RegistroMetadata

class QueryResolver:

    @staticmethod
    def resolve(template: str, ctx: TaskContext=None) -> str:
        if ctx:
            values = {}
            values.update(ctx.key)
            values.update(ctx.query_params)
            values["id_semaforo"] = ctx.task.query_params.get("id")
            values["uid"] = ctx.task.uid
            query = template
            for k, v in values.items():
                query = query.replace(f"${{{k}}}", str(v))

            return query
        else:
            return template