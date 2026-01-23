from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from metadata.loader.metadata_loader import TaskLogRepository
from processor.domain import Metric
from processor.update_strategy.registro_update_strategy import ExecutionResult


class PostTaskAction(ABC):

    @abstractmethod
    def execute(self, er: ExecutionResult, ctx):
        pass

    def required_metrics(self) -> Metric:
        pass


class UpdateRegistroAction(PostTaskAction):

    def __init__(self, strategy, registro_repo):
        self.strategy = strategy
        self.registro_repo = registro_repo

    def required_metrics(self):
        return self.strategy.required_metrics()

    def execute(self, er: ExecutionResult, ctx):
        self.strategy.update(er, ctx, self.registro_repo)


class SparkMetricsAction(PostTaskAction):

    def __init__(self, log_repo: TaskLogRepository):
        self._log_repo = log_repo

    def execute(self, er, ctx):

        df = ctx.df

        num_rows = df.count()
        num_partitions = df.rdd.getNumPartitions()
        partition_sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
        self._log_repo.insert_metric(ctx,num_rows,num_partitions,",".join(map(str, partition_sizes)))

    def required_metrics(self):
        return "spark_metrics"