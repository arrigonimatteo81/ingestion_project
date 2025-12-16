import main_processor
from common.dataproc import DataprocService
from common.result import OperationResult
from common.utils import get_logger, extract_field_from_file
from metadata.loader.metadata_loader import OrchestratorMetadata
import subprocess

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
            self._repository = OrchestratorMetadata(self._connection_string)
        else:
            self._repository = repository

        logger.info("Orchestrator initialized")

    def start(self) -> OperationResult:
        logger.info("Running orchestrator manager")
        logger.info(
            "Fetching tasks in groups " + ", ".join(self._groups) + "..."
        )
        tasks: set[str] = self._fetch_tasks_ids_in_groups(self._groups)
        if len(tasks) == 0:
            err_mex = f"No tasks found in groups '{self._groups}'"
            logger.error(err_mex)
            return OperationResult(successful=False, description=err_mex)
        else:
            logger.debug(f"Task retrieved for groups {self._groups}: {tasks}")
            todo_list = DataprocService.create_todo_list(self._config_file,self._repository,self._run_id,tasks)
            for i in todo_list:
                #print(f"{i}")
                #cmd = [
                #    "spark-submit",
                #    i['pyspark_job']['main_python_file_uri'],
                #    f"-t {i['pyspark_job']['args'][1]}", f"-r {i['pyspark_job']['args'][3]}", f"-c {i['pyspark_job']['args'][5]}", "-b False"
                #]

                #subprocess.run(cmd, check=True, text=True)
                main_processor.run_processor(i["pyspark_job"]["args"][1], i["pyspark_job"]["args"][3], i["pyspark_job"]["args"][5])
                #print(i["pyspark_job"]["main_python_file_uri"])
                #print(i["pyspark_job"]["args"][1])

                #if __name__ == '__main__':
                #    main_processor(r=self._run_id,t=i.)
            return OperationResult(successful=True, description="Tutto ok")


    def _fetch_tasks_ids_in_groups(self, groups: [str]) -> set[str]:
        str_groups=",".join(groups)
        logger.debug(f"Retrieving tasks in groups {str_groups}")
        query_tasks = self._repository.get_all_tasks_in_group(groups)
        tasks: set[str] = {item.task_id for item in query_tasks}
        return tasks

