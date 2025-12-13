import getopt
import sys

from common.result import OperationResult
from common.utils import get_logger, extract_field_from_file, download_from_gcs
from processor.manager import ProcessorManagerFactory

logger = get_logger(__name__)

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "r:t:c:b:", ["run_id=", "task_id=", "config_file=", "is_blocking="]
        )

        for opt, arg in opts:
            if opt in ("-r", "--run_id"):
                run_id = arg
            elif opt in ("-t", "--task_id"):
                task_id = arg
            elif opt in ("-c", "--config_file"):
                config_file = arg
            elif opt in ("-b", "--is_blocking"):
                is_blocking = arg.lower() == "true"
    except getopt.GetoptError:
        sys.exit(1)

    try:
        print(
            f"Starting processor_main with run_id: '{run_id}', task_id: '{task_id}', config_file: '{config_file}', is_blocking: {is_blocking}"
        )
        if config_file.lower().startswith("gs://"):
            logger.info(f"Download {config_file} from gcs...")
            download_from_gcs(config_file)
        else:
            logger.warning(
                f"Skipping download json from gcs since {config_file} doesn't start with gcs"
            )

        logger.debug("Creating transformer")
        processor = ProcessorManagerFactory.create_processor_manager(
            run_id=run_id, task_id=task_id, config_file=config_file
        )

        logger.info("Executing transformation task")
        processor_result: OperationResult = processor.start()
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

