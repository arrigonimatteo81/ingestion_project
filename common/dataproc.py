from google.api_core.retry import Retry
from google.api_core import retry as retries
from google.cloud.dataproc_v1 import WorkflowTemplate, WorkflowTemplateServiceClient, WorkflowMetadata, WorkflowGraph, \
    WorkflowNode
from google.api_core.operation import Operation
from common.configuration import DataprocConfiguration
from common.result import OperationResult
from common.task_semaforo_payload import TaskSemaforoPayload
from common.utils import get_logger
from metadata.loader.metadata_loader import OrchestratorMetadata
from metadata.models.tab_tasks import TaskSemaforo
import time

logger = get_logger(__name__)


def create_workflow_template_name(run_id: str, groups: list, index: int) -> str:
    if not groups or not run_id:
        raise ValueError(
            "Group list and run_id are mandatory to create the template name"
        )
    groups = list(set(groups))  # removing duplicates
    groups_lower = [g.lower() for g in groups]
    groups_sorted = sorted(groups_lower)
    groups_formatted = "_".join(g.replace(",", "") for g in groups_sorted)
    workflow_id = f"wft-{run_id}-{groups_formatted}-{str(index).rjust(3, '0')}"
    return workflow_id


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
                "labels": DataprocService._build_labels(task, run_id)
            }

    @staticmethod
    def _build_labels(task, run_id) -> dict:
        raw_labels = {
            "run_id": run_id,
            "table": task.key.get("cod_tabella"),
            "abi": task.key.get("cod_abi"),
            "prov": task.key.get("cod_provenienza"),
            "periodo": task.query_params.get("num_periodo_rif")
        }

        return {
            k: str(v).lower()[:63]
            for k, v in raw_labels.items()
            if v is not None
        }

    @staticmethod
    def create_todo_list(config_file: str,orchestrator_repository: OrchestratorMetadata,run_id: str, tasks: [TaskSemaforo],
                         max_tasks_per_workflow: int, dp_cfg, groups: [str])  -> list[WorkflowTemplate]:
        logger.debug("Creating todo list...")

        heavy = list(filter(lambda t: t.is_heavy, tasks))
        normal = list(filter(lambda t: not t.is_heavy, tasks))

        workflows = []
        wf_idx = 1

        while heavy or normal:
            wf_tasks = []

            if heavy:
                wf_tasks.append(heavy.pop(0))

            while len(wf_tasks) < max_tasks_per_workflow and normal:
                wf_tasks.append(normal.pop(0))

            steps = [
                DataprocService.instantiate_task(
                    task=task,
                    run_id=run_id,
                    config_file=config_file,
                    repository=orchestrator_repository
                )
                for task in wf_tasks
            ]

            workflow_id = create_workflow_template_name(run_id, groups, wf_idx)

            workflow_template: WorkflowTemplate = (
                DataprocService.create_dataproc_workflow_template(
                    tasks=steps,
                    workflow_id=workflow_id,
                    dataproc_configuration=dp_cfg
                )
            )

            workflows.append(workflow_template)

            wf_idx += 1

        return workflows

    @staticmethod
    def create_dataproc_workflow_template(
            tasks,
            workflow_id: str,
            dataproc_configuration: DataprocConfiguration
    ) -> WorkflowTemplate:
        logger.debug("Creating workflow request...")

        template_request: dict = DataprocService.create_workflow_template_request(
            dataproc_configuration,
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
            tasks,
            workflow_id: str,
    ) -> dict:

        logger.debug(f"Creating Dataproc workflow template with {len(tasks)} tasks")
        template_request = {
            "id": workflow_id,
            "placement": dataproc_configuration.cluster_name,
            "jobs": [tasks]
        }
        return template_request

    @staticmethod
    def create_workflow_client(
            dataproc_configuration: DataprocConfiguration,
    ) -> WorkflowTemplateServiceClient:
        workflow_client: WorkflowTemplateServiceClient = WorkflowTemplateServiceClient(
            client_options={"api_endpoint": f"{dataproc_configuration.region}-dataproc.googleapis.com:443"}
        )
        return workflow_client

    @staticmethod
    def instantiate_dataproc_workflow_template(
            dataproc_configuration: DataprocConfiguration, template_name: str
    ):
        logger.debug(f"Instantiating Dataproc workflow template {template_name}...")
        workflow_client: WorkflowTemplateServiceClient = (
            DataprocService.create_workflow_client(dataproc_configuration)
        )
        operation: Operation = workflow_client.instantiate_workflow_template(
            name=template_name
        )

        logger.debug(f"Workflow {template_name} started")
        while not operation.done():
            logger.debug(
                f"Waiting for the workflow ({str(dataproc_configuration.poll_sleep_time_seconds)} secs)..."
            )
            time.sleep(dataproc_configuration.poll_sleep_time_seconds)

        metadata: WorkflowMetadata = operation.metadata
        logger.debug(
            f"Workflow finished: State: {metadata.state}, Start time: {metadata.start_time}, End time: {metadata.end_time}"
        )

        return DataprocService._extract_result_from_operation(operation)

    @staticmethod
    def _extract_result_from_operation(operation: Operation) -> OperationResult:
        nodes_errors: list = []
        metadata: WorkflowMetadata = operation.metadata
        logger.debug(f"Extracting metadata from operation: {metadata}")
        if metadata:
            graph: WorkflowGraph = metadata.graph
            if graph:
                logger.debug("Job statuses:")
                all_nodes_success: bool = True

                for node in graph.nodes:
                    step_id = node.step_id
                    job_id = node.job_id
                    state = node.state
                    logger.debug(f"\tStep: {step_id}, Job: {job_id}, State: {state}")

                    if int(state) != WorkflowNode.NodeState.COMPLETED:
                        all_nodes_success = False
                        nodes_errors.append(
                            f"Step ID: {step_id}, Job ID: {job_id}, State: {state} failed with error {str(node.error)}"
                        )
                if all_nodes_success:
                    logger.info("Workflow completed successfully")
                    return OperationResult(True, f"")
                else:
                    err: str = (
                            f"Workflow terminated with errors: '"
                            + ", ".join(nodes_errors)
                            + "'"
                    )
                    logger.error(err)
                    return OperationResult(False, err)
            else:
                err: str = "No graph data in metadata"
                logger.warning(err)
                return OperationResult(False, err)
        else:
            err: str = "No metadata available in operation"
            logger.warning(err)
            return OperationResult(False, err)