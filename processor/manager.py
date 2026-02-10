from abc import ABC, abstractmethod
from typing import Optional

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.const import NAME_OF_PARTITIONING_COLUMN
from common.result import OperationResult
from common.secrets import SecretRetriever
from common.task_runtime import TaskRuntime
from common.utils import extract_field_from_file, get_logger
from factories.destination_factory import DestinationFactory
from factories.registro_update_strategy_factory import RegistroUpdateStrategyFactory
from factories.source_factory import SourceFactory
from helpers.query_resolver import TaskContext
from metadata.loader.metadata_loader import ProcessorMetadata, MetadataLoader, TaskLogRepository, \
    RegistroRepository, SemaforoMetadata
from metadata.models.tab_tasks import TaskSemaforo
from processor.destinations.base import Destination
from processor.domain import Metric
from processor.update_strategy.post_task_action import UpdateRegistroAction, SparkMetricsAction
from processor.update_strategy.registro_update_strategy import ExecutionResult

logger = get_logger(__name__)

class BaseProcessorManager (ABC):
    def __init__(self,run_id: str, task: TaskSemaforo, config_file: str,
                 layer: str, opt_secret_retriever: Optional[SecretRetriever] = None):
        self._run_id = run_id
        self._task = task
        self._config_file = config_file
        self._connection_string: str = extract_field_from_file(
            config_file, "CONNECTION_PARAMS"
        )
        self._opt_secret_retriever: Optional[SecretRetriever] = opt_secret_retriever
        self._repository = ProcessorMetadata(MetadataLoader(self._connection_string))
        self._log_repository = TaskLogRepository(MetadataLoader(self._connection_string))
        self._registro_repository = RegistroRepository(MetadataLoader(self._connection_string))
        self._semaforo_repository = SemaforoMetadata(MetadataLoader(self._connection_string))
        self._layer=layer

    def _build_runtime(self):
        """Retrieve common data needed by all processor types"""
        logger.debug(f"Retrieving transformations for task_id={self._task.uid}")

        source_type = self._repository.get_source_info(self._task.source_id)
        config_partitioning = self._repository.get_jdbc_source_partitioning_info(self._task.key, self._layer)
        task_source = SourceFactory.create_source(source_type, self._task.source_id, self._config_file, config_partitioning, self._opt_secret_retriever)
        logger.info(
            f"Source retrieved for task_id={self._task.uid}: {task_source}"
        )

        task_is_blocking: bool = self._repository.get_task_is_blocking(self._task.logical_table, self._layer)
        has_next_step: bool = self._repository.get_task_has_next(self._task.logical_table, self._layer)
        logger.info(
            f"Retrieve task info is_blocking for task_id={self._task.uid}: {task_is_blocking}"
        )

        destination_type = self._repository.get_destination_info(self._task.destination_id)
        task_destination = DestinationFactory.create_destination(destination_type, self._task.destination_id, self._config_file, self._opt_secret_retriever)
        logger.info(
            f"Destination retrieved for task_id={self._task.uid}: {task_destination}"
        )

        strategy = RegistroUpdateStrategyFactory().create(self._task.tipo_caricamento)

        post_actions = [
            UpdateRegistroAction(strategy,self._registro_repository),
            #SparkMetricsAction(self._log_repository)
        ]

        return TaskRuntime(
            source=task_source,
            destination=task_destination,
            is_blocking=task_is_blocking,
            has_next_step=has_next_step,
            post_actions=post_actions
        )


    def start(self) -> OperationResult:
        ctx = TaskContext(
            self._task,
            key=self._task.key,
            query_params=self._task.query_params,
            run_id=self._run_id
        )

        try:
            self._log_repository.insert_task_log_running(ctx, self._layer)

            tr: TaskRuntime = self._build_runtime()

            execution_result: ExecutionResult = self._execute_task(
                ctx,
                tr.source,
                tr.destination
            )

            # post actions comuni
            for action in tr.post_actions:
                action.execute(execution_result, ctx)

            # semaforo
            if tr.has_next_step and execution_result.get("row_count", 0) > 0:
                self._semaforo_repository.insert_task_semaforo(ctx, layer=self._layer)

            # log finale
            self._log_repository.insert_task_log_successful(
                ctx,
                execution_result.get("row_count", 0),
                self._layer
            )

            return OperationResult(True, "")

        except Exception as exc:
            if tr.task_is_blocking:
                self._log_repository.insert_task_log_failed(ctx, str(exc), self._layer)
            else:
                self._log_repository.insert_task_log_warning(ctx, str(exc), self._layer)

            logger.error(exc, exc_info=True)
            return OperationResult(False, str(exc))

    @abstractmethod
    def _execute_task(self, ctx, source, destination) -> ExecutionResult:
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

    def _execute_task(self, ctx, source, destination):

        session = self._get_spark_session()
        df = source.to_dataframe(session, ctx)
        df = df.drop(NAME_OF_PARTITIONING_COLUMN)
        destination.write(df)
        max_data = df.agg(
            F.max("num_data_va").alias("max_data")
        ).collect()[0]["max_data"]

        rowcount = df.count()
        return ExecutionResult({
            "max_data_va": max_data,
            "row_count": rowcount
        })

class NativeProcessorManager (BaseProcessorManager):

    def _get_spark_session(self) -> SparkSession:
        spark = (SparkSession.builder
                 .config("spark.executor.instances", "1")
                 .config("spark.executor.cores", "1")
                 .config("spark.executor.memory", "512m")
                 .config("spark.sql.shuffle.partitions", "1")
                 .config("spark.default.parallelism", "1")
                 .appName(f"Processor_{self._run_id}_{self._task.uid}")
                 .getOrCreate())
        logger.debug(
            f"SparkSession properties: {spark.sparkContext.getConf().getAll()}"
        )
        return spark

    def _execute_task(self, ctx, source, destination):
        s = self._get_spark_session
        res_read = source.fetch_all(ctx)
        destination.write_rows(res_read)
        max_data = max(row['num_data_va'] for row in res_read) if res_read else None
        rowcount = len(res_read)
        return ExecutionResult({
            "max_data_va": max_data,
            "row_count": rowcount
        })

class BigQueryProcessorManager (BaseProcessorManager):
    def _execute_task(self, ctx, source, destination):
        query = source.to_query(ctx)
        row_number = destination.write_query(query, ctx)
        return ExecutionResult({
            "row_count": row_number
        })