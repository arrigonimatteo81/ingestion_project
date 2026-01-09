from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from processor.update_strategy.registro_update_strategy import ExecutionResult


class PostTaskAction(ABC):

    @abstractmethod
    def execute(self, df: DataFrame, ctx):
        pass

class UpdateRegistroAction(PostTaskAction):

    def __init__(self, strategy):
        self.strategy = strategy

    def execute(self, er: ExecutionResult, ctx):
        self.strategy.update(er, ctx)