from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame

from helpers.query_resolver import TaskContext


class SparkReadable(ABC):
    @abstractmethod
    def to_dataframe(self, spark: SparkSession, ctx: TaskContext) -> DataFrame:
        pass


class NativeReadable(ABC):
    @abstractmethod
    def fetch_all(self, ctx: TaskContext ):
        pass


class BigQueryReadable(ABC):
    @abstractmethod
    def to_query(self, ctx: TaskContext ) -> str:
        pass


