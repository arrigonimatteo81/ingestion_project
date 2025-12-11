from pyspark.sql import SparkSession
import getopt
import sys
from common.utils import get_logger, download_from_gcs
from metadata.loader.metadata_loader import MetadataLoader
from ingestion.orchestrator import IngestionOrchestrator

logger = get_logger(__name__)

if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "r:g:c:", ["run_id=", "groups=", "config_file="]
        )

        run_id = None
        groups = []  # Initialize groups as an empty list
        config_file = None

        for opt, arg in opts:
            if opt in ("-r", "--run_id"):
                run_id = arg
            elif opt in ("-g", "--groups"):
                groups = arg.split(",")  # Split the CSV string into a list
            elif opt in ("-c", "--config_file"):
                config_file = arg

        print(
            f"Starting main_orchestrator with run_id: {run_id}, groups: {groups}, config_file: {config_file}"
        )

        if config_file.lower().startswith("gs:"):
            logger.info(f"Download {config_file} from gcs...")
            download_from_gcs(config_file)
        else:
            logger.warn(
                f"Skipping download json from gcs since {config_file} doesn't start with gcs"
            )