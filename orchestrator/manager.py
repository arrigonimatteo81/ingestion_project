from google.cloud.dataproc_v1 import WorkflowTemplate

from common.configuration import DataprocConfiguration, Configuration, OrchestratorConfiguration
from common.dataproc import DataprocService
from common.result import OperationResult
from common.utils import get_logger, extract_field_from_file
from metadata.loader.metadata_loader import OrchestratorMetadata, MetadataLoader
from metadata.models.tab_tasks import TaskSemaforo

logger = get_logger(__name__)


class OrchestratorManager:

    def __init__(self, run_id: str, config_file: str, groups: [str] = None,
                 repository: OrchestratorMetadata = None):
        logger.debug(
            f"Initializing Orchestrator with run_id '{run_id}', config_file '{config_file}', groups '{groups}', repository '{repository}'"
        )
        self._run_id = run_id
        self._config_file = config_file
        self._groups = [] if groups is None else groups
        self._connection_string: str = extract_field_from_file(config_file, "CONNECTION_PARAMS")

        if repository is None:
            logger.debug("Initializing the db and repository")
            self._repository = OrchestratorMetadata(MetadataLoader(self._connection_string))
        else:
            self._repository = repository

        conf = Configuration(self._repository.get_all_configurations())
        self._dataproc_cfg = DataprocConfiguration.from_configuration(conf)
        self._orchestrator_cfg = OrchestratorConfiguration.from_configuration(conf)

        logger.info("Orchestrator initialized")

    def _fetch_tasks_ids_in_groups(self, groups: [str]) -> [TaskSemaforo]:
        str_groups=",".join(groups)
        logger.debug(f"Retrieving tasks in groups {str_groups}")
        tasks = self._repository.get_all_tasks_in_group(groups)
        return tasks

    def start(self) -> OperationResult:
        logger.info("Running orchestrator manager")
        logger.info(
            "Fetching tasks in groups " + ", ".join(self._groups) + "..."
        )
        tasks: [TaskSemaforo] = self._fetch_tasks_ids_in_groups(self._groups)
        if len(tasks) == 0:
            err_mex = f"No tasks found in groups '{self._groups}'"
            logger.error(err_mex)
            return OperationResult(successful=False, description=err_mex)
        else:
            logger.debug(f"Task retrieved for groups {self._groups}: {tasks}")
            todo_list = DataprocService.create_todo_list(self._config_file,self._repository,self._run_id,tasks,
                                                         int(self._orchestrator_cfg.ingestion_max_contemporary_tasks),
                                                         self._orchestrator_cfg.bucket)

            workflow_id = DataprocService.create_workflow_template_name(self._run_id, self._groups, 1)

            workflow_template: WorkflowTemplate =DataprocService.create_dataproc_workflow_template(todo_list, workflow_id,self._dataproc_cfg)
            result: OperationResult = (
                DataprocService.instantiate_dataproc_workflow_template(
                    self._dataproc_cfg, workflow_template.name
                )
            )
            if not result.successful:
                return OperationResult(successful=False, description=f"{result.description}")

            return OperationResult(successful=True, description="Tutto ok")












