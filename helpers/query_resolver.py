from dataclasses import dataclass

from metadata.loader.metadata_loader import RegistroMetadata
from metadata.models.tab_tasks import TaskSemaforo


@dataclass(frozen=True)
class TaskContext:
    task: TaskSemaforo
    key: dict
    query_params: dict
    registro_repo: RegistroMetadata

class QueryResolver:

    @staticmethod
    def resolve(template: str, ctx: TaskContext=None) -> str:
        if ctx:
            values = {}
            values.update(ctx.key)
            values.update(ctx.query_params)
            values["id_semaforo"] = ctx.task.query_params.get("id")

            query = template
            for k, v in values.items():
                query = query.replace(f"${{{k}}}", str(v))

            return query
        else:
            return template