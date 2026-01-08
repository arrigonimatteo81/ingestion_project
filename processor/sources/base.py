from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame

from db.database import DbConcrete
from helpers.query_resolver import TaskContext


class Source(ABC):

    @abstractmethod
    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        pass

    def fetch_all(self, ctx: TaskContext = None):
        raise NotImplementedError

