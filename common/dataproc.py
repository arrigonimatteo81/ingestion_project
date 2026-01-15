from google.api_core.retry import Retry
from google.api_core import retry as retries
from google.cloud.dataproc_v1 import WorkflowTemplate

from common.configuration import DataprocConfiguration
from common.task_semaforo_payload import TaskSemaforoPayload
from common.utils import get_logger
from metadata.loader.metadata_loader import OrchestratorMetadata
from metadata.models.tab_tasks import TaskSemaforo

logger = get_logger(__name__)



class DataprocService:
    @staticmethod
    def instantiate_task (task: TaskSemaforo, repository: OrchestratorMetadata, run_id: str, config_file: str) -> dict:
        logger.debug(f"Instantiating task: {task} ...")
        payload: TaskSemaforoPayload = TaskSemaforoPayload(task.uid, task.source_id, task.destination_id, task.tipo_caricamento,
                            task.key, task.query_params)
        task_type = repository.get_task_configuration(task.key)

        return {
                "step_id": f"step-{task.uid}",
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
    def create_todo_list(config_file: str,orchestrator_repository: OrchestratorMetadata,run_id: str, tasks: [TaskSemaforo]):
        logger.debug("Creating todo list...")
        list_of_tasks=[]
        for task in tasks:
            list_of_tasks.append(DataprocService.instantiate_task(task, orchestrator_repository, run_id, config_file))
        return list_of_tasks

    @staticmethod
    def create_dataproc_workflow_template(
            tasks,
            workflow_id: str,
            dataproc_configuration: DataprocConfiguration,
            orchestrator_repository: OrchestratorMetadata,
            run_id: str,
            config_file: str,

    ) -> WorkflowTemplate:
        logger.debug("Creating workflow request...")
        template_request: dict = DataprocService.create_workflow_template_request(
            dataproc_configuration,
            run_id,
            tasks,
            workflow_id,
        )
        logger.debug("Creating workflow template through client...")
        workflow_client = DataprocService.create_workflow_client(dataproc_configuration)
        parent = f"projects/{dataproc_configuration.project}/regions/{dataproc_configuration.region}"
        retry_policy = Retry(
            initial=1.0,
            maximum=10.0,
            multiplier=2,
            deadline=60,  # Total timeout in seconds
            predicate=retries.if_exception_type(
                # Specific exceptions to retry on
                ConnectionError,
                TimeoutError,
            ),
        )
        wf_template: WorkflowTemplate = workflow_client.create_workflow_template(
            parent=parent, template=template_request, retry=retry_policy
        )
        logger.debug(f"Workflow template {wf_template.name} successfully created")
        return wf_template

    @staticmethod
    def create_workflow_template_request(
            dataproc_configuration: DataprocConfiguration,
            run_id: str,
            tasks,
            workflow_id: str,
    ) -> dict:

        logger.debug(f"Creating Dataproc workflow template with {len(tasks)} tasks")
        template_request = {
            "id": workflow_id,
            "placement": dataproc_configuration.cluster_name,
            "jobs": [],  # Initialize an empty list for jobs
        }
        for task in tasks:
            template_request["jobs"].append(task)

        return template_request