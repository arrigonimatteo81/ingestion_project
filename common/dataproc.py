import shlex

from google.api_core.retry import Retry
from google.api_core import retry as retries
from google.cloud import storage
from google.cloud.dataproc_v1 import WorkflowTemplate, WorkflowTemplateServiceClient, WorkflowMetadata, WorkflowGraph, \
    WorkflowNode, WorkflowTemplatePlacement, ManagedCluster, ClusterSelector
from google.api_core.operation import Operation
from common.configuration import DataprocConfiguration
from common.result import OperationResult
from common.task_semaforo_payload import TaskSemaforoPayload
from common.utils import get_logger
from metadata.loader.metadata_loader import OrchestratorMetadata
from metadata.models.tab_tasks import TaskSemaforo
import time

logger = get_logger(__name__)





class DataprocService:

    @staticmethod
    def upload_task_payload(payload: TaskSemaforoPayload, bucket_name: str, object_prefix: str) -> str:
        """
        Salva il payload JSON su GCS e ritorna il path completo gs://bucket/object
        """
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Genera un object name unico
        object_name = f"{object_prefix}/{payload.uid}.json"
        blob = bucket.blob(object_name)

        # Scrive il JSON su GCS
        blob.upload_from_string(payload.to_json(), content_type="application/json")

        return f"{bucket_name}/{object_name}"


    @staticmethod
    def instantiate_task (task: TaskSemaforo, repository: OrchestratorMetadata, run_id: str, config_file: str, bucket_name: str, layer: str) -> dict:
        logger.debug(f"Instantiating task: {task} ...")
        payload: TaskSemaforoPayload = TaskSemaforoPayload(task.uid, task.logical_table, task.source_id, task.destination_id, task.tipo_caricamento,
                            task.key, task.query_params)

        task_file_path = DataprocService.upload_task_payload(payload, bucket_name, object_prefix=f"tasks_payloads/{layer}")
        task_type = repository.get_task_configuration(task.key, layer)

        return {
                "step_id": f"step-{task.uid}",
                "pyspark_job": {
                    "main_python_file_uri": task_type.main_python_file_uri,
                    "args": [
                        "--run_id",
                        run_id,
                        "--task",
                        task_file_path,
                        "--config_file",
                        config_file,
                        "--is_blocking",
                        str(True),
                        "--layer",
                        layer
                    ],
                    "python_file_uris": task_type.additional_python_file_uris,
                    "jar_file_uris": task_type.jar_file_uris,
                    "file_uris": task_type.additional_file_uris,
                    "properties": task_type.dataproc_properties
                },
                "labels": DataprocService._build_labels(task, run_id, layer)
            }

    @staticmethod
    def create_workflow_template_name(run_id: str, groups: list, idx: int) -> str:
        if not groups or not run_id:
            raise ValueError(
                "Group list and run_id are mandatory to create the template name"
            )
        groups = list(set(groups))  # removing duplicates
        groups_lower = [g.lower() for g in groups]
        groups_sorted = sorted(groups_lower)
        groups_formatted = "_".join(g.replace(",", "") for g in groups_sorted)
        workflow_id = f"wft-{run_id}-{groups_formatted}-{str(idx).rjust(2,'0')}"
        return workflow_id

    @staticmethod
    def _build_labels(task, run_id, layer) -> dict:
        raw_labels = {
            "run_id": run_id,
            "table": task.key.get("cod_tabella"),
            "abi": task.key.get("cod_abi"),
            "prov": task.key.get("cod_provenienza"),
            "periodo": task.query_params.get("num_periodo_rif"),
            "layer": layer
        }

        return {
            k: str(v).lower()[:63]
            for k, v in raw_labels.items()
            if v is not None
        }

    @staticmethod
    def create_todo_list(config_file: str,orchestrator_repository: OrchestratorMetadata,run_id: str, tasks: [TaskSemaforo],
                         max_tasks_per_workflow: int, bucket: str, layer: str):

        logger.debug("Creating todo list...")

        heavy_tasks = [t for t in tasks if t.is_heavy]
        light_tasks = [t for t in tasks if not t.is_heavy]

        steps = []
        previous_heavy_step_id = None

        for task in heavy_tasks:
            step = DataprocService.instantiate_task(task, orchestrator_repository, run_id,config_file, bucket, layer)

            if previous_heavy_step_id:
                step["prerequisite_step_ids"] = [previous_heavy_step_id]

            steps.append(step)
            previous_heavy_step_id = step["step_id"]

        if len(heavy_tasks) > 0:
            num_light_slots = max_tasks_per_workflow - 1
        else:
            num_light_slots = max_tasks_per_workflow

        slots = [[] for _ in range(num_light_slots)]

        for idx, task in enumerate(light_tasks):
            slots[idx % num_light_slots].append(task)

        for slot in slots:
            previous_step_id = None

            for task in slot:
                step = DataprocService.instantiate_task(task, orchestrator_repository, run_id,config_file, bucket, layer)

                if previous_step_id:
                    step["prerequisite_step_ids"] = [previous_step_id]

                steps.append(step)
                previous_step_id = step["step_id"]

        return steps

    @staticmethod
    def create_dataproc_workflow_template(
            tasks,
            workflow_id: str,
            dataproc_configuration: DataprocConfiguration
    ) -> WorkflowTemplate:
        logger.debug("Creating workflow request...")

        template_request: WorkflowTemplate = DataprocService.create_workflow_template_request(
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
    ) -> WorkflowTemplate:

        logger.debug(f"Creating Dataproc workflow template with {len(tasks)} tasks")
        return WorkflowTemplate(
        id=workflow_id,
        placement=WorkflowTemplatePlacement(
            cluster_selector=ClusterSelector(
                cluster_labels={
                    "acronimo": "nplg0"
                }
            )
        ),
        jobs=tasks
        )

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