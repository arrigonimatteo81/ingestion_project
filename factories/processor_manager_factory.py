from typing import Optional

from common.secrets import SecretRetriever
from common.utils import extract_field_from_file, get_logger
from metadata.loader.metadata_loader import ProcessorMetadata, MetadataLoader
from metadata.models.tab_tasks import TaskSemaforo
from processor.domain import ProcessorType
from processor.manager import SparkProcessorManager, BaseProcessorManager, NativeProcessorManager, \
    BigQueryProcessorManager

logger = get_logger(__name__)

class ProcessorManagerFactory:
    @staticmethod
    def create_processor_manager(run_id: str, task: TaskSemaforo, config_file: str,
            opt_secret_retriever: Optional[SecretRetriever] = None) -> BaseProcessorManager:
        connection_string = extract_field_from_file(config_file, "CONNECTION_PARAMS")
        repository = ProcessorMetadata(MetadataLoader(connection_string))
        try:
            processor_type = repository.get_task_processor_type(task.key)
            logger.debug(f"Processor type for task {task.uid}: {processor_type}")

            if processor_type.upper() == ProcessorType.SPARK.value:
               return SparkProcessorManager(
                run_id=run_id,
                task=task,
                config_file=config_file,
                opt_secret_retriever=opt_secret_retriever
                )
            elif processor_type.upper() == ProcessorType.NATIVE.value:
               return NativeProcessorManager(
                run_id=run_id,
                task=task,
                config_file=config_file,
                opt_secret_retriever=opt_secret_retriever
                )
            elif processor_type.upper() == ProcessorType.BIGQUERY.value:
               return BigQueryProcessorManager(
                run_id=run_id,
                task=task,
                config_file=config_file,
                opt_secret_retriever=opt_secret_retriever
                )
            else:
                logger.error("Unsupported processor type!!!")
                raise ValueError(f"Unsupported processor type: {processor_type}")
        except Exception as exc:
            logger.error(f"Failed to create processor manager: {exc}")
            raise