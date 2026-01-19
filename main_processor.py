import getopt
import sys

from common.result import OperationResult
from common.secrets import SecretRetrieverFactory
from common.task_semaforo_payload import TaskSemaforoPayload
from common.utils import get_logger, extract_field_from_file, download_from_gcs
from factories.processor_manager_factory import ProcessorManagerFactory
from metadata.models.tab_tasks import TaskSemaforo

logger = get_logger(__name__)


def run_processor(run_id,task,config_file):
    logger.debug("Reading secret_retriever_configuration field")
    secret_retriever_configuration: dict = extract_field_from_file(file_path=config_file,
                                                                   field_name="secrets.secret_retriever")
    logger.debug(f"Secret retriever configuration: {secret_retriever_configuration}")
    file_secret_retriever = SecretRetrieverFactory.from_dict(secret_retriever_configuration)
    logger.debug(f"Created file_secret_retriever: {str(file_secret_retriever)}")

    logger.debug("Creating transformer")
    processor = ProcessorManagerFactory.create_processor_manager(
        run_id=run_id, task=task, config_file=config_file,
            opt_secret_retriever=file_secret_retriever
    )
    logger.info("Executing transformation task")
    processor_result: OperationResult = processor.start()
    return processor_result


if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "r:t:c:b:", ["run_id=", "task_id=", "config_file=", "is_blocking="]
        )

        for opt, arg in opts:
            if opt in ("-r", "--run_id"):
                run_id = arg
            elif opt in ("-t", "--task"):
                task_payload = TaskSemaforoPayload.from_json(arg)
                task: TaskSemaforo = task_payload.to_domain()
            elif opt in ("-c", "--config_file"):
                config_file = arg
            elif opt in ("-b", "--is_blocking"):
                is_blocking = arg.lower() == "true"
    except getopt.GetoptError:
        sys.exit(1)

    try:
        print(
            f"Starting processor_main with run_id: {run_id}, task_id: {task.uid}, config_file: {config_file}, is_blocking: {is_blocking}"
        )
        if config_file.lower().startswith("gs://"):
            logger.info(f"Download {config_file} from gcs...")
            download_from_gcs(config_file)
        else:
            logger.warning(
                f"Skipping download json from gcs since {config_file} doesn't start with gcs"
            )

        processor_result = run_processor(run_id,task,config_file)
        logger.info(
            f"transformation task completed exited successfully: {processor_result.successful}"
        )
        if processor_result.successful:
            sys.exit(0)
        else:
            # logger.error(processor_result.description)
            # sys.exit(1)
            if is_blocking:
                logger.error(f"The job failed. Exited with an error: {processor_result.description}")
                sys.exit(1)
            else:
                logger.warning("The job failed, but is NOT blocking. Orchestrator continues.")
                sys.exit(0)
    except Exception as ex:
        logger.error(ex, exc_info=True)
        sys.exit(1)

