from common.utils import extract_field_from_file, get_logger, format_key_for_task_configuration
from metadata.loader.metadata_loader import ProcessorMetadata
from metadata.models.tab_tasks import TaskSemaforo
from processor.domain import ProcessorType
from processor.manager import SparkProcessorManager, BaseProcessorManager

logger = get_logger(__name__)

class ProcessorManagerFactory:
    @staticmethod
    def create_processor_manager(run_id: str, task: TaskSemaforo, config_file: str) -> BaseProcessorManager:
        connection_string = extract_field_from_file(config_file, "CONNECTION_PARAMS")
        repository = ProcessorMetadata(connection_string)
        try:
            processor_type = repository.get_task_processor_type(format_key_for_task_configuration(task.source_id,task.cod_abi,task.cod_provenienza))
            logger.debug(f"Processor type for task {task.uid}: {processor_type}")

            if processor_type.upper() == ProcessorType.SPARK.value:
               return SparkProcessorManager(
                run_id=run_id,
                task=task,
                config_file=config_file,
                )
            else:
                logger.error("Unsupported processor type!!!")
                raise ValueError(f"Unsupported processor type: {processor_type}")
        except Exception as exc:
            logger.error(f"Failed to create processor manager: {exc}")
            raise