import getopt
import sys
from google.cloud import storage
from common.result import OperationResult
from common.secrets import SecretRetrieverFactory
from common.task_semaforo_payload import TaskSemaforoPayload
from common.utils import get_logger, extract_field_from_file, download_from_gcs
from factories.processor_manager_factory import ProcessorManagerFactory
from metadata.models.tab_tasks import TaskSemaforo
from processor.domain import Layers

logger = get_logger(__name__)


def run_processor(run_id,task,config_file, layer):
    logger.debug("Reading secret_retriever_configuration field")
    secret_retriever_configuration: dict = extract_field_from_file(file_path=config_file,
                                                                   field_name="secrets.secret_retriever")
    logger.debug(f"Secret retriever configuration: {secret_retriever_configuration}")
    file_secret_retriever = SecretRetrieverFactory.from_dict(secret_retriever_configuration)
    logger.debug(f"Created file_secret_retriever: {str(file_secret_retriever)}")

    logger.debug("Creating transformer")
    processor = ProcessorManagerFactory.create_processor_manager(
        run_id=run_id, task=task, config_file=config_file,layer=layer,
            opt_secret_retriever=file_secret_retriever
    )
    logger.info("Executing transformation task")
    processor_result: OperationResult = processor.start()
    return processor_result

def show_usage():
    print(
        f"Usage: {sys.argv[0]} --run_id <run_id> --task_id <task_id> --config_file <config_file> --is_blocking <<true|false>>'\n"
        f"Example: {sys.argv[0]} --run_id 202408281226 --task_id 1 --config_file gs://mybucket/application.conf --is_blocking false"
    )


def load_task_payload(file_path:str) -> TaskSemaforo:
    client = storage.Client()
    bucket_name, blob_name = file_path.replace("gs://", "").split("/", 1)
    content = client.bucket(bucket_name).blob(blob_name).download_as_text()
    payload = TaskSemaforoPayload.from_json(content)
    return payload.to_domain()

def delete_task_file(file_path: str):
    """
    Cancella il file JSON da GCS.
    """
    client = storage.Client()
    bucket_name, blob_name = file_path.replace("gs://", "").split("/", 1)
    client.bucket(bucket_name).blob(blob_name).delete()

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "r:t:c:b:l:", ["run_id=", "task=", "config_file=", "is_blocking=", "layer="]
        )

        for opt, arg in opts:
            if opt in ("-r", "--run_id"):
                run_id = arg
            elif opt in ("-t", "--task"):
                task_file=arg
                task: TaskSemaforo = load_task_payload(task_file)

            elif opt in ("-c", "--config_file"):
                config_file = arg
            elif opt in ("-b", "--is_blocking"):
                is_blocking = arg.lower() == "true"
            elif opt in ("-l", "--layer"):
                layer = arg # == Layers.STAGE.value
    except getopt.GetoptError:
        show_usage()
        sys.exit(1)

    try:
        print(
            f"Starting processor_main with run_id: {run_id}, task_id: {task.uid}, config_file: {config_file}, is_blocking: {is_blocking}, layer: {layer}"
        )
        if config_file.lower().startswith("gs://"):
            logger.info(f"Download {config_file} from gcs...")
            download_from_gcs(config_file)
        else:
            logger.warning(
                f"Skipping download json from gcs since {config_file} doesn't start with gcs"
            )

        processor_result = run_processor(run_id,task,config_file, layer)
        logger.info(
            f"transformation task completed exited successfully: {processor_result.successful}"
        )
        if processor_result.successful:
            #delete_task_file(task_file)
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

