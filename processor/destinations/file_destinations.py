import re

from pyspark.sql import DataFrame

from metadata.models.tab_file import GCS
from processor.destinations.base import Destination
from processor.domain import FileFormat


class FileDestination(GCS, Destination):
    def __init__(self, format_file: FileFormat, gcs_path: str, overwrite: bool):
        GCS.__init__(self, format_file, gcs_path)
        Destination.__init__(self, overwrite)

    def write(self, df: DataFrame):
        pass

class CsvFileDestination(FileDestination):
    def __init__(self, gcs_path: str, separator: str, overwrite: bool):
        FileDestination.__init__(self, FileFormat.CSV, gcs_path, overwrite)
        self.separator = separator

    def write(self, df: DataFrame):
        if self.overwrite:
            df.write.csv(path=self.gcs_path, header=True, sep=self.separator, mode="overwrite")
        else:
            df.write.csv(path=self.gcs_path, sep=self.separator, header=True, mode="append")


class ParquetFileDestination(FileDestination):
    def __init__(self, gcs_path: str, overwrite: bool):
        FileDestination.__init__(self, FileFormat.PARQUET, gcs_path, overwrite)

    def write(self, df: DataFrame):
        if self.overwrite:
            df.write.parquet(path=self.gcs_path, mode="overwrite")
        else:
            df.write.parquet(path=self.gcs_path, mode="append")