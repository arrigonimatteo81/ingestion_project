from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from processor.domain import Metric
from processor.update_strategy.registro_update_strategy import ExecutionResult


class PostTaskAction(ABC):

    @abstractmethod
    def execute(self, er: ExecutionResult, ctx):
        pass

    def required_metrics(self) -> Metric:
        pass


class UpdateRegistroAction(PostTaskAction):

    def __init__(self, strategy):
        self.strategy = strategy

    def required_metrics(self):
        return self.strategy.required_metrics()

    def execute(self, er: ExecutionResult, ctx):
        self.strategy.update(er, ctx)