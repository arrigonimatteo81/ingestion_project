from common.task_semaforo_payload import TaskSemaforoPayload
from common.utils import get_logger, format_key_for_task_configuration
from metadata.loader.metadata_loader import OrchestratorMetadata

logger = get_logger(__name__)



class DataprocService:
    @staticmethod
    def instantiate_task (task_id: str, repository: OrchestratorMetadata, run_id: str, config_file: str) -> dict:
        logger.debug(f"Instantiating task: {task_id} ...")
        task = repository.get_task(task_id)
        payload: TaskSemaforoPayload = TaskSemaforoPayload(task.uid, task.id, task.cod_abi, task.source_id, task.destination_id, task.cod_provenienza,
                            task.num_periodo_rif, task.cod_gruppo, task.cod_colonna_valore, task.num_ambito,
                            task.num_max_data_va)
        task_type = repository.get_task_configuration(format_key_for_task_configuration(task.source_id,task.cod_abi,task.cod_provenienza))
        return {
                "step_id": f"step-{task_id}",
                "pyspark_job": {
                    "main_python_file_uri": task_type.main_python_file_uri,
                    "args": [
                        "--run_id",
                        run_id,
                        "--task",
                        payload.to_json(),
                        "--config_file",
                        config_file,
                        "--is_blocking",
                        str(True)
                    ],
                    "python_file_uris": task_type.additional_python_file_uris,
                    "jar_file_uris": task_type.jar_file_uris,
                    "file_uris": task_type.additional_file_uris,
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

