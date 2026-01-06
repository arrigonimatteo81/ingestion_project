from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class PostTaskAction(ABC):

    @abstractmethod
    def execute(self, df: DataFrame, ctx):
        pass

class UpdateRegistroAction(PostTaskAction):

    def __init__(self, strategy):
        self.strategy = strategy

    def execute(self, df, ctx):
        self.strategy.update(df, ctx)