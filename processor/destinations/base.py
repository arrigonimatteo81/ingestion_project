from abc import abstractmethod, ABC
from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class Destination:
    def __init__(self, overwrite: bool):
        self._overwrite = overwrite

    @property
    def overwrite(self):
        return self._overwrite


class SparkWritable(ABC):

    @abstractmethod
    def write(self, df: DataFrame):
        pass


class NativeWritable(ABC):
    @abstractmethod
    def write_rows(self, rows):
        pass


class BigQueryWritable(ABC):
    @abstractmethod
    def write_query(self, query, ctx):
        pass