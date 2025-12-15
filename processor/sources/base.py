from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame


class Source(ABC):

    @abstractmethod
    def to_dataframe(self, spark: SparkSession) -> DataFrame:
        pass

