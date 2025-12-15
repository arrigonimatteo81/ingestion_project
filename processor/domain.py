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

