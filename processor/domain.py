from enum import Enum


class ProcessorType(Enum):
    BIGQUERY = "BIGQUERY"
    SPARK = "SPARK"

class SourceType(Enum):
    JDBC = "JDBC"
    FILE = "FILE"
    BIGQUERY = "BIGQUERY"

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
