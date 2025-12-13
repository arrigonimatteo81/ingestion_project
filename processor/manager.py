from abc import ABC, abstractmethod

from common.result import OperationResult
from common.utils import extract_field_from_file, get_logger
from metadata.loader.metadata_loader import ProcessorMetadata
from metadata.models.tab_src import TabSrc
from processor.domain import ProcessorType

logger = get_logger(__name__)


class BaseProcessorManager (ABC):
    def __init__(self,run_id: str, task_id: str, config_file: str):
        self._run_id = run_id
        self._task_id = task_id
        self._config_file = config_file
        self._connection_string: str = extract_field_from_file(
            config_file, "CONNECTION_PARAMS"
        )
        self._repository = ProcessorMetadata(self._connection_string)
        #self._task_state_manager = TaskLogManager(repository=self._repository)

    def _get_common_data(self):
        """Retrieve common data needed by all processor types"""
        logger.debug(f"Retrieving transformations for task_id={self._task_id}")

        task_source: TabSrc = self._repository.get_source(self._task_id)
        logger.info(
            f"Transformation retrieved for task_id={self._task_id}: {task_source}"
        )

        task: Task = self._repository.get_task(self._task_id)
        logger.info(
            f"Retrieve task for task_id={self._task_id}: {task}"
        )

        task_destination: TabDest = self._repository.get_destination(task_id=self._task_id)

        logger.info(f"task_destination: {task_destination}")

        return task_source, task, task_destination

    @abstractmethod
    def start(self) -> OperationResult:
        pass


class SparkProcessorManager (BaseProcessorManager):
    pass


class ProcessorManagerFactory():
    @staticmethod
    def create_processor_manager(run_id: str, task_id: str, config_file: str):
        connection_string = extract_field_from_file(config_file, "CONNECTION_PARAMS")
        repository = ProcessorMetadata(connection_string)
        try:
            processor_type = repository.get_task_processor_type(task_id)
            logger.debug(f"Processor type for task {task_id}: {processor_type}")

            if processor_type.upper() == ProcessorType.SPARK.value:
               return SparkProcessorManager(
                run_id=run_id,
                task_id=task_id,
                config_file=config_file,
                )
            else:
                logger.error("Unsupported processor type!!!")
                raise ValueError(f"Unsupported processor type: {processor_type}")
        except Exception as exc:
            logger.error(f"Failed to create processor manager: {exc}")
            raise