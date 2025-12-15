from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from common.result import OperationResult
from common.utils import extract_field_from_file, get_logger
from metadata.loader.metadata_loader import ProcessorMetadata
from metadata.models.tab_jdbc import TabJDBCSource, TabJDBCDest
from processor.destinations.base import Destination
from processor.destinations.jdbc_destinations import TableJDBCDestination
from processor.domain import ProcessorType, SourceType, DestinationType
from processor.sources.base import Source
from processor.sources.jdbc_sources import TableJDBCSource, QueryJDBCSource

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

        source_id,source_type = self._repository.get_source_info(self._task_id)
        task_source: Source = SourceFactory.create_source(source_type, source_id, self._config_file)
        logger.info(
            f"Source retrieved for task_id={self._task_id}: {task_source}"
        )

        task_is_blocking: bool = self._repository.get_task_is_blocking(self._task_id)
        logger.info(
            f"Retrieve task info is_blocking for task_id={self._task_id}: {task_is_blocking}"
        )

        destination_id, destination_type = self._repository.get_destination_info(self._task_id)
        task_destination: Destination = DestinationFactory.create_destination(destination_type, destination_id, self._config_file)
        logger.info(
            f"Destination retrieved for task_id={self._task_id}: {task_source}"
        )

        return task_source, task_is_blocking, task_destination

    @abstractmethod
    def start(self) -> OperationResult:
        pass


class SparkProcessorManager (BaseProcessorManager):

    def _get_spark_session(self) -> SparkSession:
        spark = SparkSession.builder.appName(
            f"Processor_{self._run_id}_{self._task_id}"
        ).getOrCreate()
        logger.debug(
            f"SparkSession properties: {spark.sparkContext.getConf().getAll()}"
        )
        return spark

    def start(self) -> OperationResult:
        try:
            task_source, task_is_blocking, task_destination = self._get_common_data()

            df = task_source.to_dataframe(self._get_spark_session())
            task_destination.write(df)
            return OperationResult(successful=True, description="")

        except Exception as exc:
            #if task_is_blocking.is_blocking:
                #self._task_state_manager.insert_task_log_failed(task_id=self._task_id, run_id=self._run_id,
                #                                                description=f"task {self._task_id} in ERROR!",
                #                                                error_message=exc.__str__())
            #else:
                #self._task_state_manager.insert_task_log_failed_not_blocking(task_id=self._task_id, run_id=self._run_id,
                #                                                             description=f"task {self._task_id} in ERROR but is not blocking",
                #                                                             error_message=exc.__str__())
            logger.error(exc, exc_info=True)
            return OperationResult(False, str(exc))


class ProcessorManagerFactory:
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

class SourceFactory:
    @staticmethod
    def create_source(source_type: str, source_id: str, config_file: str) -> Source:
        connection_string = extract_field_from_file(config_file, "CONNECTION_PARAMS")
        repository = ProcessorMetadata(connection_string)
        if source_type.upper() == SourceType.JDBC.value:
            jdbc_source :TabJDBCSource = repository.get_jdbc_source_info(source_id)
            if jdbc_source.tablename:
                return TableJDBCSource(jdbc_source.username,jdbc_source.pwd, jdbc_source.driver, jdbc_source.url, jdbc_source.tablename)
            elif jdbc_source.query_text:
                return QueryJDBCSource(jdbc_source.username, jdbc_source.pwd, jdbc_source.driver, jdbc_source.url, jdbc_source.query_text)
        #if source_type.upper() == SourceType.FILE.value:

        #if source_type.upper() == SourceType.BIGQUERY.value:

class DestinationFactory:
    @staticmethod
    def create_destination(destination_type: str, destination_id: str, config_file: str) -> Destination:
        connection_string = extract_field_from_file(config_file, "CONNECTION_PARAMS")
        repository = ProcessorMetadata(connection_string)
        if destination_type.upper() == DestinationType.JDBC.value:
            jdbc_destination :TabJDBCDest = repository.get_jdbc_dest_info(destination_id)
            return TableJDBCDestination(jdbc_destination.username,jdbc_destination.pwd, jdbc_destination.driver,
                                        jdbc_destination.url, jdbc_destination.tablename, jdbc_destination.overwrite)
        #if source_type.upper() == SourceType.FILE.value:

        #if source_type.upper() == SourceType.BIGQUERY.value: