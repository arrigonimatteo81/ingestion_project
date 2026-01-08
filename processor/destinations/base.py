from abc import abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class Destination:
    def __init__(self, overwrite: bool):
        self._overwrite = overwrite

    @property
    def overwrite(self):
        return self._overwrite

    @abstractmethod
    def write(self, df: DataFrame):
        pass

    @abstractmethod
    def write_rows(self, rows):
        raise NotImplementedError