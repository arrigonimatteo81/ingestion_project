from abc import ABC, abstractmethod

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.result import OperationResult
from common.utils import extract_field_from_file, get_logger
from factories.destination_factory import DestinationFactory
from factories.registro_update_strategy_factory import RegistroUpdateStrategyFactory
from factories.source_factory import SourceFactory
from helpers.query_resolver import TaskContext
from metadata.loader.metadata_loader import ProcessorMetadata, MetadataLoader, TaskLogRepository, \
    RegistroRepository
from metadata.models.tab_tasks import TaskSemaforo
from processor.destinations.base import Destination
from processor.domain import Metric
from processor.update_strategy.post_task_action import UpdateRegistroAction
from processor.update_strategy.registro_update_strategy import ExecutionResult

logger = get_logger(__name__)

class BaseProcessorManager (ABC):
    def __init__(self,run_id: str, task: TaskSemaforo, config_file: str):
        self._run_id = run_id
        self._task = task
        self._config_file = config_file
        self._connection_string: str = extract_field_from_file(
            config_file, "CONNECTION_PARAMS"
        )
        self._repository = ProcessorMetadata(MetadataLoader(self._connection_string))
        self._log_repository = TaskLogRepository(MetadataLoader(self._connection_string))
        self._registro_repository = RegistroRepository(MetadataLoader(self._connection_string))

    def _get_common_data(self):
        """Retrieve common data needed by all processor types"""
        logger.debug(f"Retrieving transformations for task_id={self._task.uid}")

        source_id,source_type = self._repository.get_source_info(self._task.uid)
        config_partitioning = self._repository.get_jdbc_source_partitioning_info(self._task.key)
        task_source = SourceFactory.create_source(source_type, source_id, self._config_file, config_partitioning)
        logger.info(
            f"Source retrieved for task_id={self._task.uid}: {task_source}"
        )

        task_is_blocking: bool = True #self._repository.get_task_is_blocking(self._task_id)
        logger.info(
            f"Retrieve task info is_blocking for task_id={self._task.uid}: {task_is_blocking}"
        )

        destination_id, destination_type = self._repository.get_destination_info(self._task.uid)
        task_destination = DestinationFactory.create_destination(destination_type, destination_id, self._config_file)
        logger.info(
            f"Destination retrieved for task_id={self._task.uid}: {task_destination}"
        )

        strategy = RegistroUpdateStrategyFactory().create(self._task.tipo_caricamento)

        post_actions = [
            UpdateRegistroAction(strategy,self._registro_repository),
        ]
        
        return task_source, task_is_blocking, task_destination, post_actions

    @abstractmethod
    def start(self) -> OperationResult:
        pass


class SparkProcessorManager (BaseProcessorManager):

    def _get_spark_session(self) -> SparkSession:
        spark = (SparkSession.builder
                 .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
                 .appName(f"Processor_{self._run_id}_{self._task.uid}")
                 .getOrCreate())
        logger.debug(
            f"SparkSession properties: {spark.sparkContext.getConf().getAll()}"
        )
        return spark

    def start(self) -> OperationResult:
        try:
            ctx = TaskContext(
                self._task,
                key=self._task.key,
                query_params=self._task.query_params,
                run_id=self._run_id
            )
            self._log_repository.insert_task_log_running(ctx)
            logger.debug(f"inizio {self._task.uid}, {self._run_id}")
            task_source, task_is_blocking, task_destination, post_actions = self._get_common_data()

            session = self._get_spark_session()
            df = task_source.to_dataframe(session, ctx)
            task_destination.write(df)
            for action in post_actions:
                required=action.required_metrics()
                if required == Metric.MAX_DATA_VA:
                    er: ExecutionResult = ExecutionResult(df.agg(F.max("num_data_va").alias("max_data")).collect()[0]["max_data"])
                else:
                    er:ExecutionResult = ExecutionResult()
                action.execute(er, ctx)
            self._log_repository.insert_task_log_successful(ctx, df.count())
            logger.debug(f"task {self._task.uid} concluso con successo")

            return OperationResult(successful=True, description="")

        except Exception as exc:
            if task_is_blocking:
                self._log_repository.insert_task_log_failed(ctx ,exc.__str__())
            else:
                self._log_repository.insert_task_log_warning(ctx, exc.__str__())
            logger.error(exc, exc_info=True)
            return OperationResult(False, str(exc))

class NativeProcessorManager (BaseProcessorManager):

    def start(self) -> OperationResult:
        try:
            ctx = TaskContext(
                self._task,
                key=self._task.key,
                query_params=self._task.query_params,
                run_id=self._run_id
            )
            logger.debug(f"inizio {self._task.uid}, {self._run_id} instanziando NativeProcessorManager")
            self._log_repository.insert_task_log_running(ctx)
            logger.debug(f"inizio {self._task.uid}, {self._run_id}")
            task_source, task_is_blocking, task_destination, post_actions = self._get_common_data()
            res_read = task_source.fetch_all(ctx)
            task_destination.write_rows(res_read)
            self._log_repository.insert_task_log_successful(ctx, len(res_read))
            logger.debug(f"task {self._task.uid} concluso con successo")

            return OperationResult(successful=True, description="")

        except Exception as exc:
            if task_is_blocking:
                self._log_repository.insert_task_log_failed(ctx ,exc.__str__())
            else:
                self._log_repository.insert_task_log_warning(ctx, exc.__str__())
            logger.error(exc, exc_info=True)
            return OperationResult(False, str(exc))

class BigQueryProcessorManager (BaseProcessorManager):

    def start(self) -> OperationResult:
        try:
            ctx = TaskContext(
                self._task,
                key=self._task.key,
                query_params=self._task.query_params,
                run_id=self._run_id
            )
            self._log_repository.insert_task_log_running(ctx)
            logger.debug(f"inizio {self._task.uid}, {self._run_id}")
            task_source, task_is_blocking, task_destination, post_actions = self._get_common_data()
            src = task_source.to_query(ctx)
            task_destination.write_query(src,ctx)
            #for action in post_actions: potrebbe però servire qui il salvataggio in registro
            #    required=action.required_metrics()
            #    if required == Metric.MAX_DATA_VA:
            #        er: ExecutionResult = ExecutionResult(df.agg(F.max("num_data_va").alias("max_data")).collect()[0]["max_data"])
            #    else:
            #        er:ExecutionResult = ExecutionResult()
            #    action.execute(er, ctx)
            self._log_repository.insert_task_log_successful(ctx, len(src))
            logger.debug(f"task {self._task.uid} concluso con successo")

            return OperationResult(successful=True, description="")

        except Exception as exc:
            if task_is_blocking:
                self._log_repository.insert_task_log_failed(ctx ,exc.__str__())
            else:
                self._log_repository.insert_task_log_warning(ctx, exc.__str__())
            logger.error(exc, exc_info=True)
            return OperationResult(False, str(exc))





