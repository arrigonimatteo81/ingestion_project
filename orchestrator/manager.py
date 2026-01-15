import subprocess

from google.cloud.dataproc_v1 import WorkflowTemplate

from common.configuration import DataprocConfiguration, Configuration
from common.const import MAX_TASKS_PER_WORKFLOW
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

        logger.info("Orchestrator initialized")

    @staticmethod
    def create_workflow_template_name(run_id: str, groups: list) -> str:
        if not groups or not run_id:
            raise ValueError(
                "Group list and run_id are mandatory to create the template name"
            )
        groups = list(set(groups))  # removing duplicates
        groups_lower = [g.lower() for g in groups]
        groups_sorted = sorted(groups_lower)
        groups_formatted = "_".join(g.replace(",", "") for g in groups_sorted)
        workflow_id = f"wft-{run_id}-{groups_formatted}"
        return workflow_id

    @staticmethod
    def chunked(tasks, size):
        for i in range(0, len(tasks), size):
            yield tasks[i:i + size]

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
            todo_list = DataprocService.create_todo_list(self._config_file,self._repository,self._run_id,tasks)

            chunks = list(self.chunked(todo_list, MAX_TASKS_PER_WORKFLOW))

            for idx, chunk in enumerate(chunks):
                workflow_template: WorkflowTemplate = (
                    DataprocService.create_dataproc_workflow_template(
                        tasks = chunk,
                        workflow_id=self.create_workflow_template_name(self._run_id, self._groups),
                        dataproc_configuration=self._dataproc_cfg,
                        orchestrator_repository=self._repository,
                        run_id=self._run_id,
                        config_file=self._config_file,
                    )
                )

            """
            #TODO utilizzare il percorso relativo del file almeno fino alla cartella venv
            #venv_python = r'C:\Users\Utente\PyCharmProjects\ingestion_project\venv\Scripts\python.exe'
            # TODO eliminare se non si gira in locale con venv
            # f"--conf=spark.pyspark.python={venv_python}",
            # f"--conf=spark.pyspark.driver.python={venv_python}",
            conf_args = []
            for i in todo_list:
                for k, v in i['pyspark_job']["properties"].items():
                    conf_args.extend(["--conf", f"{k}={v}"])
                #"--master", "local[*]",
                cmd = [
                     "spark-submit",
                     "--jars", ",".join(i['pyspark_job']['jar_file_uris']),
                     *conf_args,
                     f"{i['pyspark_job']['main_python_file_uri']}",
                     "-r", f"{i['pyspark_job']['args'][1]}",
                     "-t", f"{i['pyspark_job']['args'][3]}",
                     "-c", f"{i['pyspark_job']['args'][5]}",
                     "-b", f"{i['pyspark_job']['args'][7]}"
                 ]

                subprocess.run(cmd, check=True, text=True, shell=False)
                #subprocess.run(cmd, check=True, text=True, shell=True)"""

            return OperationResult(successful=True, description="Tutto ok")


    def _fetch_tasks_ids_in_groups(self, groups: [str]) -> [TaskSemaforo]:
        str_groups=",".join(groups)
        logger.debug(f"Retrieving tasks in groups {str_groups}")
        tasks = self._repository.get_all_tasks_in_group(groups)
        return tasks

