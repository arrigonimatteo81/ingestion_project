from enum import Enum


class ProcessorType(Enum):
    BIGQUERY = "BIGQUERY"
    SPARK = "SPARK"
    NATIVE = "NATIVE"

class SourceType(Enum):
    JDBC = "JDBC"
    FILE = "FILE"
    BIGQUERY = "BIGQUERY"

class FileFormat(Enum):
    EXCEL = "EXCEL"
    PARQUET = "PARQUET"
    CSV = "CSV"
    AVRO = "AVRO"

class DestinationType(Enum):
    JDBC = "JDBC"
    FILE = "FILE"
    BIGQUERY = "BIGQUERY"

class TaskState(Enum):
    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCESSFUL = "SUCCESSFUL"
    WARNING = "WARNING"
