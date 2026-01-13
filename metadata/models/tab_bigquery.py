from dataclasses import dataclass

@dataclass
class TabBigQuerySource:
    url: str
    username: str
    pwd: str
    driver: str
    tablename: str
    query_text: str = None
    partitioning_expression: str = None
    num_partitions: int = None

    def __repr__(self):
        return (f"TabBigQuerySource(url={self.url},username={self.username},pwd={self.pwd},driver={self.driver},"
                f"tablename={self.tablename}, query_text={self.query_text},partitioning_expression={self.partitioning_expression},"
                f"num_partitions={self.num_partitions})")

@dataclass
class TabBigQueryDest:
    url: str
    username: str
    pwd: str
    driver: str
    tablename: str
    columns: [str]
    overwrite: bool = False

    def __repr__(self):
        return (f"TabBigQueryDest(url={self.url},username={self.username},pwd={self.pwd},driver={self.driver},"
                f"tablename={self.tablename}, overwrite={self.overwrite}, columns={self.columns})")

class BigQuery:

    format = "bigquery"

    def __init__(self, project: str, dataset: str):
        self._project = project
        self._dataset = dataset

    @property
    def project(self):
        return self._project

    @property
    def dataset(self):
        return self._dataset

class BigQueryTable(BigQuery):
    def __init__(self,project: str, dataset: str, table: str):

        # Initialize the base JDBC class with the provided credentials and connection details.
        BigQuery.__init__(self, project,dataset)
        # Set the specific database table for this instance.
        self.table = table

class BigQueryQuery(BigQuery):
    def __init__(self, project: str, dataset: str, query: str):
        # Initialize the base JDBC class with the provided credentials and connection details.
        BigQuery.__init__(self, project, dataset)
        self.query = query