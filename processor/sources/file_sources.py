import re

import pandas as pd
from google.cloud import storage
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from helpers.query_resolver import TaskContext
from metadata.models.tab_file import GCS
from processor.domain import FileFormat
from processor.sources.base import SparkReadable


class FileSource(GCS, SparkReadable):
    def __init__(self, format_file: FileFormat, gcs_path: str,):
        GCS.__init__(self, format_file, gcs_path)

    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        pass

class ExcelFileSource(FileSource):
    def __init__(self, gcs_path: str, sheet: str):
        FileSource.__init__(self, FileFormat.EXCEL, gcs_path)
        self.bucket_name = re.match(r"^gs://([^/]+)(/.*)$", self.gcs_path).group(1)
        self.blob_name = re.match(r"^gs://([^/]+)(/.*)$", self.gcs_path).group(2)[1:]
        self.sheet = sheet

    def __repr__(self):
        return (f"ExcelGCSSource(format_file: {self.format_file}, gcs_path: {self.gcs_path}, "
                f"bucket_name: {self.bucket_name}), blob_name: {self.blob_name}, sheet: {self.sheet})")

    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.blob_name)

        df_read = self._readfile(blob)
        df_ret: DataFrame = spark.createDataFrame(df_read)
        df_ret = df_ret.withColumn("id_processo",col(ctx.task.uid))
        return df_ret

    def _readfile(self, blob):
        data_bytes = blob.download_as_bytes()

        df = pd.read_excel(data_bytes, sheet_name=self.sheet, index_col=None)
        return df

class CsvFileSource(FileSource):
    def __init__(self, gcs_path: str, separator: str):
        FileSource.__init__(self, FileFormat.CSV, gcs_path)
        #self.bucket_name = re.match(r"^gs://([^/]+)(/.*)$", self.gcs_path).group(1)
        self.separator = separator

    def __repr__(self):
        return f"CsvGCSSource(format_file: {self.format_file}, gcs_path: {self.gcs_path}, separator: {self.separator})"

    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        df_read:DataFrame = (spark.read.format("csv")
                .option("path",self.gcs_path)
                .option("sep",self.separator)
                .option("inferSchema",True)
                .option("header",True)
                .load())

        return df_read.withColumn("id_processo", col(ctx.task.uid))

class ParquetFileSource(FileSource):
    def __init__(self, gcs_path: str):
        FileSource.__init__(self, FileFormat.PARQUET, gcs_path)
        #self.bucket_name = re.match(r"^gs://([^/]+)(/.*)$", self.gcs_path).group(1)

    def __repr__(self):
        return f"ParquetGCSSource(format_file: {self.format_file}, gcs_path: {self.gcs_path}, "
                #f"bucket_name: {self.bucket_name})")

    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        df_read:DataFrame =  spark.read.format("parquet").option("path",self.gcs_path).load()
        return df_read.withColumn("id_processo", col(ctx.task.uid))