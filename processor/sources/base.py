from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame

from helpers.query_resolver import TaskContext


"""class Source(ABC):

    @abstractmethod
    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        pass

    def fetch_all(self, ctx: TaskContext = None):
        raise NotImplementedError"""

class SparkReadable(ABC):
    @abstractmethod
    def to_dataframe(self, spark, ctx):
        pass


class NativeReadable(ABC):
    @abstractmethod
    def fetch_all(self, ctx):
        pass


class BigQueryReadable(ABC):
    @abstractmethod
    def to_query(self, ctx) -> str:
        pass


