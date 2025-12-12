from dataclasses import dataclass

from common.utils import get_logger
from metadata.loader.metadata_loader import OrchestratorMetadata
from metadata.models.tab_tasks import Task

logger = get_logger(__name__)



class DataprocService:
    @staticmethod
    def instantiate_task (task_id: str, repository: OrchestratorMetadata, run_id: str, config_file: str) -> dict:
        logger.debug(f"Instantiating task: {task_id} ...")
        task = repository.get_task(task_id)
        task_type = repository.get_task_configuration(task.config_profile)
        return {
                "step_id": f"step-{task_id}",
                "pyspark_job": {
                    "main_python_file_uri": task_type.main_python_file,
                    "args": [
                        "--run_id",
                        run_id,
                        "--task_id",
                        task_id,
                        "--config_file",
                        config_file,
                        "--is_blocking",
                        str(task.is_blocking)
                    ],
                    "python_file_uris": task_type.python_file_uris,
                    "jar_file_uris": task_type.jar_file_uris,
                    "file_uris": task_type.file_uris,
                    "properties": task_type.dataproc_properties
                },
            }


    @staticmethod
    def create_todo_list(config_file: str,orchestrator_repository: OrchestratorMetadata,run_id: str, tasks: set[str]):
        logger.debug("Creating todo list...")
        list_of_tasks=[]
        for task_id in tasks:
            list_of_tasks.append(DataprocService.instantiate_task(task_id, orchestrator_repository, run_id, config_file))
        return list_of_tasks

